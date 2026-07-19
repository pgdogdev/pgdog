#!/usr/bin/env bash
# hyperfine --prepare: reset schema, COPY empty tables (creates/reuses slot), fill sources.
#
# The COPY phase runs against empty tables — trivial, just establishes the slot
# at the current LSN. fill.sql then queues the WAL backlog the timed command drains.
# Slot lifecycle (bench_copy_slot_replication) is owned by this script:
# dropped at the start of each prepare so every run starts from a fresh LSN.

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
echo ">>>>> [1/6] dropping stale replication slots"
echo "============================================================"
for db in "${SOURCE_DBS[@]}"; do
    psql -d "${db}" -qX -c \
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'bench_copy%' AND NOT active;"
done

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
    psql -d "${db}" -qX -c "DROP SCHEMA IF EXISTS bench_copy CASCADE" > /dev/null
done

echo ""
echo "============================================================"
echo ">>>>> [4/6] establishing replication slot via COPY (empty tables)"
echo "============================================================"
# COPY empty tables; creates the slot at the current LSN.  Exit immediately after COPY (--sync-only).
"${PGDOG_BIN}" \
    --config "${PGDOG_CONFIG}" \
    --users  "${SETUP_DIR}/users.toml" \
    data-sync \
    --from-database source \
    --to-database   destination \
    --publication   bench_copy \
    --replication-slot bench_copy_slot_replication \
    --sync-only

echo ""
echo "============================================================"
echo ">>>>> [5/6] filling source shards in parallel (WAL backlog)"
echo "============================================================"
FILL_PIDS=()
for db in "${SOURCE_DBS[@]}"; do
    psql -d "${db}" -qX -f "${SETUP_DIR}/fill.sql" &
    FILL_PIDS+=("$!")
done
echo "  waiting for all fills to complete..."
wait "${FILL_PIDS[@]}"

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

echo ""
echo "============================================================"
echo ">>>>> WAL backlog + sentinels ready"
echo "============================================================"
echo ""
