#!/bin/bash
# Integration test: data-sync retries when a destination shard connection drops mid-copy.
#
# What is tested:
#   - data-sync --sync-only completes (exit 0) when shard_1 is killed mid-copy and
#     brought back while the retry loop is running.
#   - Row counts on all destination tables match the source after sync completes.
#
# Manages its own docker-compose stack — no pre-started containers required.
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
COMPOSE_DIR="${SCRIPT_DIR}/.."
DEFAULT_BIN="${SCRIPT_DIR}/../../../target/debug/pgdog"
PGDOG_BIN=${PGDOG_BIN:-$DEFAULT_BIN}
PGDOG_CONFIG="${SCRIPT_DIR}/pgdog.toml"
USERS_CONFIG="${SCRIPT_DIR}/users.toml"
export PGPASSWORD=pgdog

SYNC_PID=""

cleanup() {
    if [ -n "${SYNC_PID}" ]; then
        kill "${SYNC_PID}" 2>/dev/null || true
        wait "${SYNC_PID}" 2>/dev/null || true
    fi
    cd "${COMPOSE_DIR}" && docker compose down 2>/dev/null || true
}
trap cleanup EXIT

pushd "${COMPOSE_DIR}"

# Start the docker-compose stack and wait for all three postgres instances.
echo "[retry_test] Starting docker-compose stack..."
docker compose up -d

echo "[retry_test] Waiting for postgres instances to be ready..."
for PORT in 15432 15433 15434; do
    READY_ATTEMPTS=0
    until pg_isready -h 127.0.0.1 -p "${PORT}" -q 2>/dev/null; do
        READY_ATTEMPTS=$((READY_ATTEMPTS + 1))
        if [ "${READY_ATTEMPTS}" -ge 60 ]; then
            echo "[retry_test] FAIL: postgres on port ${PORT} not ready after 30s"
            exit 1
        fi
        sleep 0.5
    done
done
echo "[retry_test] All postgres instances ready."

# Reset destination shards then source — each is a separate container.
psql -h 127.0.0.1 -p 15433 -U pgdog pgdog1 -c "DROP SCHEMA IF EXISTS copy_data CASCADE;"
psql -h 127.0.0.1 -p 15434 -U pgdog pgdog2 -c "DROP SCHEMA IF EXISTS copy_data CASCADE;"
psql -h 127.0.0.1 -p 15432 -U pgdog pgdog -f "${SCRIPT_DIR}/init.sql"

# Schema sync: create tables on destination shards.
"${PGDOG_BIN}" --config "${PGDOG_CONFIG}" --users "${USERS_CONFIG}" \
    schema-sync --from-database source --to-database destination --publication pgdog

# Start data-sync with all shards up so the connection pool initialises cleanly
# and CopySubscriber can connect to both shards before we inject a failure.
"${PGDOG_BIN}" --config "${PGDOG_CONFIG}" --users "${USERS_CONFIG}" \
    data-sync --sync-only \
    --from-database source \
    --to-database destination \
    --publication pgdog &
SYNC_PID=$!

# Wait until data-sync has created a temporary replication slot on the source.
# The slot is created after copy_sub.connect() — meaning CopySubscriber already
# holds open connections to both shards. Killing shard_1 now lands a mid-copy
# network error that run_with_retry() must handle, not a pre-connection timeout.
echo "[retry_test] Waiting for data-sync to start copying..."
WAIT_ATTEMPTS=0
until psql -h 127.0.0.1 -p 15432 -U pgdog pgdog -tAc \
    "SELECT 1 FROM pg_replication_slots WHERE temporary = true LIMIT 1" 2>/dev/null | grep -q 1; do
    WAIT_ATTEMPTS=$((WAIT_ATTEMPTS + 1))
    if [ "${WAIT_ATTEMPTS}" -ge 200 ]; then
        echo "[retry_test] FAIL: data-sync never created a replication slot after $((WAIT_ATTEMPTS / 20))s"
        exit 1
    fi
    if ! kill -0 "${SYNC_PID}" 2>/dev/null; then
        echo "[retry_test] FAIL: data-sync exited before copy started"
        exit 1
    fi
    sleep 0.05
done

# SIGKILL the container immediately — no grace period — so the kill lands before
# the in-flight COPY can finish. docker compose stop has a 10s grace period.
echo "[retry_test] Killing shard_1 during active copy..."
docker compose kill shard_1

# Let the retry loop run a few cycles before bringing shard_1 back.
sleep 2

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
docker compose down
popd
