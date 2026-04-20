#!/bin/bash
# Integration test: data-sync retries when a destination shard is temporarily unavailable.
#
# What is tested:
#   - data-sync --sync-only completes (exit 0) when shard_1 is stopped before the
#     sync starts and brought back while retries are in flight.
#   - Row counts on all destination tables match the source after sync completes.
#
# Requires: the copy_data docker-compose stack to be running.
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEFAULT_BIN="${SCRIPT_DIR}/../../target/debug/pgdog"
PGDOG_BIN=${PGDOG_BIN:-$DEFAULT_BIN}
PGDOG_CONFIG="${SCRIPT_DIR}/pgdog.retry_test.toml"
USERS_CONFIG="${SCRIPT_DIR}/users.toml"
export PGPASSWORD=pgdog

SYNC_PID=""

cleanup() {
    if [ -n "${SYNC_PID}" ]; then
        kill "${SYNC_PID}" 2>/dev/null || true
        wait "${SYNC_PID}" 2>/dev/null || true
    fi
    # Always bring shard_1 back on exit so the stack is not left broken.
    cd "${SCRIPT_DIR}" && docker compose start shard_1 2>/dev/null || true
}
trap cleanup EXIT

pushd "${SCRIPT_DIR}"

# Reset destination and reload source data.
psql -h 127.0.0.1 -p 15432 -U pgdog pgdog -f init.sql

# Schema sync: create tables on destination shards.
"${PGDOG_BIN}" --config "${PGDOG_CONFIG}" --users "${USERS_CONFIG}" \
    schema-sync --from-database source --to-database destination --publication pgdog

# Stop shard_1 before the copy starts.
# Every table copy connects to all destination shards, so all tables will fail on the
# first attempt and enter the retry loop.
echo "[retry_test] Stopping shard_1..."
docker compose stop shard_1

# Start data-sync in the background.
"${PGDOG_BIN}" --config "${PGDOG_CONFIG}" --users "${USERS_CONFIG}" \
    data-sync --sync-only \
    --from-database source \
    --to-database destination \
    --publication pgdog &
SYNC_PID=$!

# Let data-sync start and hit connection failures on shard_1.
sleep 2

# Bring shard_1 back while retries are in flight.
echo "[retry_test] Starting shard_1..."
docker compose start shard_1

# Wait for shard_1 postgres to be ready.
READY_ATTEMPTS=0
until pg_isready -h 127.0.0.1 -p 15434 -U pgdog -d pgdog2 -q; do
    READY_ATTEMPTS=$((READY_ATTEMPTS + 1))
    if [ "${READY_ATTEMPTS}" -ge 120 ]; then
        echo "[retry_test] FAIL: shard_1 not ready after $((READY_ATTEMPTS / 2))s"
        exit 1
    fi
    sleep 0.5
done
echo "[retry_test] shard_1 is ready."

# Wait for data-sync to complete.
set +e
wait "${SYNC_PID}"
SYNC_EXIT=$?
set -e
SYNC_PID=""

if [ "${SYNC_EXIT}" -ne 0 ]; then
    echo "[retry_test] FAIL: data-sync exited with code ${SYNC_EXIT}"
    exit "${SYNC_EXIT}"
fi

# Verify row counts.
# Sharded tables: sum across both destination shards must equal source.
SHARDED_TABLES="copy_data.users copy_data.orders copy_data.order_items copy_data.log_actions copy_data.with_identity"
# Omni tables: each shard must have the full source row count.
OMNI_TABLES="copy_data.countries copy_data.currencies copy_data.categories"

FAILED=0

for TABLE in ${SHARDED_TABLES}; do
    SRC=$(psql -h 127.0.0.1 -p 15432 -U pgdog pgdog -tAc "SELECT COUNT(*) FROM ${TABLE}")
    DST0=$(psql -h 127.0.0.1 -p 15433 -U pgdog pgdog1 -tAc "SELECT COUNT(*) FROM ${TABLE}")
    DST1=$(psql -h 127.0.0.1 -p 15434 -U pgdog pgdog2 -tAc "SELECT COUNT(*) FROM ${TABLE}")
    DST=$((DST0 + DST1))
    if [ "${SRC}" -ne "${DST}" ]; then
        echo "[retry_test] MISMATCH ${TABLE}: source=${SRC} total_dest=${DST} (shard0=${DST0} shard1=${DST1})"
        FAILED=1
    else
        echo "[retry_test] OK ${TABLE}: ${SRC} rows"
    fi
done

for TABLE in ${OMNI_TABLES}; do
    SRC=$(psql -h 127.0.0.1 -p 15432 -U pgdog pgdog -tAc "SELECT COUNT(*) FROM ${TABLE}")
    DST0=$(psql -h 127.0.0.1 -p 15433 -U pgdog pgdog1 -tAc "SELECT COUNT(*) FROM ${TABLE}")
    DST1=$(psql -h 127.0.0.1 -p 15434 -U pgdog pgdog2 -tAc "SELECT COUNT(*) FROM ${TABLE}")
    if [ "${SRC}" -ne "${DST0}" ] || [ "${SRC}" -ne "${DST1}" ]; then
        echo "[retry_test] MISMATCH ${TABLE} (omni): source=${SRC} shard0=${DST0} shard1=${DST1}"
        FAILED=1
    else
        echo "[retry_test] OK ${TABLE} (omni): ${SRC} rows on each shard"
    fi
done

if [ "${FAILED}" -ne 0 ]; then
    echo "[retry_test] FAIL: row count mismatches detected."
    exit 1
fi

echo "[retry_test] PASS: all row counts match. Retry test succeeded."
popd
