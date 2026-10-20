#!/usr/bin/env bash
# Timed command: stream WAL backlog to completion, detected via sentinel rows.
#
# Starts pgdog in --replicate-only mode (no COPY; slot was established in prepare),
# then polls all 4 destination shards for the sentinel row written by prepare.sh.
# When the sentinel is present on every shard, replication has drained the entire
# backlog.  pgdog is then stopped cleanly and the script exits 0.
#
# PGDOG_BIN and SETUP_DIR are exported by run.sh via bench_run().
set -euo pipefail

DEST_DBS=(shard_0 shard_1 shard_2 shard_3)
# Sentinel id written by prepare.sh into bench_copy.files on every source.
SENTINEL_ID=-1

# Inherit PGDOG_CONFIG from run.sh (defaults to pgdog.toml for standalone use).
PGDOG_CONFIG="${PGDOG_CONFIG:-${SETUP_DIR}/pgdog.toml}"
POLL_INTERVAL=0.1  # seconds between destination polls

# Start replication in the background.  Schema sync is skipped because prepare.sh
# already ran schema-sync and created the slots.
"${PGDOG_BIN}" \
    --config "${PGDOG_CONFIG}" \
    --users  "${SETUP_DIR}/users.toml" \
    data-sync \
    --from-database source \
    --to-database   destination \
    --publication   bench_copy \
    --replication-slot bench_copy_slot_replication \
    --skip-schema-sync \
    --replicate-only &
PGDOG_PID=$!

stop_pgdog() {
    kill "${PGDOG_PID}" 2>/dev/null || return 0
    for _ in $(seq 100); do
        kill -0 "${PGDOG_PID}" 2>/dev/null || return 0
        sleep 0.05
    done
    kill -9 "${PGDOG_PID}" 2>/dev/null || true
}

# Ensure pgdog is stopped on any exit path; returns as soon as it exits (max 5s before SIGKILL).
trap stop_pgdog EXIT

echo "  pgdog replication started (pid=${PGDOG_PID}), polling for sentinels..."

# Poll until the sentinel row appears on all destination shards.
while true; do
    all_found=true
    for db in "${DEST_DBS[@]}"; do
        count=$(psql -d "${db}" -qXAt -c \
            "SELECT COUNT(*) FROM bench_copy.files WHERE id = ${SENTINEL_ID}" 2>/dev/null || echo 0)
        if [[ "${count}" -lt 1 ]]; then
            all_found=false
            break
        fi
    done
    $all_found && break
    sleep "${POLL_INTERVAL}"
done

echo "  all sentinels confirmed — replication backlog drained"
