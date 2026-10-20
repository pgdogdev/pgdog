#!/usr/bin/env bash
# hyperfine --prepare: reset schemas, sync the destination schema, create slots, fill sources.
#
# The destination schema comes from pgdog schema-sync (pre-data, then post-data).
# The slots are created with SQL, one per source shard, named "bench_copy_slot_replication_<shard>"
# as pgdog expects. No data-sync runs here, so nothing else writes WAL between the
# slot creation and the fill. fill.sql then queues the WAL backlog the timed command
# drains: few large INSERT ... SELECT transactions, then BENCH_SCALE/25 single-row
# transactions. Every bench_copy slot is dropped at the start of each prepare so every
# run starts from a fresh LSN. A CHECKPOINT after the drop lets Postgres recycle the
# old WAL, and a CHECKPOINT at the end keeps checkpoint I/O out of the timed command.

set -euo pipefail

# Inherit PGDOG_CONFIG from run.sh (defaults to pgdog.toml for standalone use).
# SETUP_DIR is also exported by run.sh.
PGDOG_CONFIG="${PGDOG_CONFIG:-${SETUP_DIR}/pgdog.toml}"

SOURCE_DBS=(pgdog1 pgdog2 pgdog3)
DEST_DBS=(shard_0 shard_1 shard_2 shard_3)

echo "============================================================"
echo ">>>>> prepare"
echo "============================================================"

echo ""
echo "============================================================"
echo ">>>>> [1/6] dropping replication slots and recycling WAL"
echo "============================================================"
psql -d "${SOURCE_DBS[0]}" -qX -v ON_ERROR_STOP=1 <<'SQL'
SELECT pg_terminate_backend(active_pid)
FROM pg_replication_slots
WHERE slot_name LIKE 'bench_copy%' AND active_pid IS NOT NULL;
DO $$
BEGIN
    FOR _ IN 1..100 LOOP
        EXIT WHEN NOT EXISTS (
            SELECT 1 FROM pg_replication_slots WHERE slot_name LIKE 'bench_copy%' AND active
        );
        PERFORM pg_sleep(0.1);
    END LOOP;
END
$$;
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name LIKE 'bench_copy%';
CHECKPOINT;
SELECT pg_switch_wal();
CHECKPOINT;
SQL

echo ""
echo "============================================================"
echo ">>>>> [2/6] resetting source schema (empty tables)"
echo "============================================================"
for db in "${SOURCE_DBS[@]}"; do
    psql -d "${db}" -qX -f "${SETUP_DIR}/setup.sql"
done

echo ""
echo "============================================================"
echo ">>>>> [3/6] resetting destination schema"
echo "============================================================"
for db in "${DEST_DBS[@]}"; do
    psql -d "${db}" -qX -c "SET client_min_messages TO warning" -c "DROP SCHEMA IF EXISTS bench_copy CASCADE" > /dev/null
done

echo ""
echo "============================================================"
echo ">>>>> [4/6] syncing destination schema and creating replication slots"
echo "============================================================"
# --data-sync-complete selects the post-data phase; older pgdog versions have no --phase.
for phase_args in "" "--data-sync-complete"; do
    # shellcheck disable=SC2086
    "${PGDOG_BIN}" \
        --config "${PGDOG_CONFIG}" \
        --users  "${SETUP_DIR}/users.toml" \
        schema-sync \
        --from-database source \
        --to-database   destination \
        --publication   bench_copy \
        ${phase_args}
done

for shard in "${!SOURCE_DBS[@]}"; do
    psql -d "${SOURCE_DBS[${shard}]}" -qX -v ON_ERROR_STOP=1 -c \
        "SELECT pg_create_logical_replication_slot('bench_copy_slot_replication_${shard}', 'pgoutput')" > /dev/null
    echo "  [${SOURCE_DBS[${shard}]}] slot bench_copy_slot_replication_${shard} created"
done

echo ""
echo "============================================================"
echo ">>>>> [5/6] filling source shards in parallel (WAL backlog: big + small transactions)"
echo "============================================================"
FILL_PIDS=()
for db in "${SOURCE_DBS[@]}"; do
    psql -d "${db}" -qX -v ON_ERROR_STOP=1 -f "${SETUP_DIR}/fill.sql" &
    FILL_PIDS+=("$!")
done
echo "  waiting for all fills to complete..."
for pid in "${FILL_PIDS[@]}"; do
    wait "${pid}"
done

echo ""
echo "============================================================"
echo ">>>>> [6/6] inserting replication sentinels into each source"
echo "============================================================"
# Insert the same sentinel row on every source.  bench_copy.files is the omni table
# (no tenant_id) so it replicates to all destination shards.  id = -1 is outside the
# generate_series range (1..scale/100) so it never collides with fill data.
for db in "${SOURCE_DBS[@]}"; do
    psql -d "${db}" -qX -c \
        "INSERT INTO bench_copy.files (id, name, mime_type, content, size_bytes, checksum, uploaded_at) \
         VALUES (-1, 'sentinel', 'application/octet-stream', decode('', 'hex'), 0, 'sentinel', now())"
    echo "  [${db}] sentinel inserted"
done

psql -d "${SOURCE_DBS[0]}" -qX -v ON_ERROR_STOP=1 -c "CHECKPOINT"

echo ""
echo "============================================================"
echo ">>>>> WAL backlog + sentinels ready"
echo "============================================================"
echo ""
