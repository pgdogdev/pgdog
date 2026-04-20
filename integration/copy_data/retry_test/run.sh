#!/bin/bash
# Integration test: COPY_DATA survives a mid-copy shard outage and a concurrent pool RELOAD,
# then confirms replication delivers writes made after the copy completes.
#
# Manages its own docker-compose stack and pgdog server process.
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
COMPOSE_DIR="${SCRIPT_DIR}/.."
DEFAULT_BIN="${SCRIPT_DIR}/../../../target/debug/pgdog"
PGDOG_BIN=${PGDOG_BIN:-$DEFAULT_BIN}
PGDOG_CONFIG="${SCRIPT_DIR}/pgdog.toml"
USERS_CONFIG="${SCRIPT_DIR}/users.toml"
PGDOG_PORT=${PGDOG_PORT:-6440}
export PGPASSWORD=pgdog

# Worst case: remaining retry backoff (≤16s) + copy of ~60k rows (~5s) + replication startup (~1s).
REPLICATION_TIMEOUT=60

PGDOG_PID=""

cleanup() {
    if [ -n "${PGDOG_PID}" ]; then
        kill "${PGDOG_PID}" 2>/dev/null || true
        wait "${PGDOG_PID}" 2>/dev/null || true
    fi
    cd "${COMPOSE_DIR}" && docker compose down 2>/dev/null || true
}
trap cleanup EXIT

# Admin connection using the [admin] section credentials from pgdog.toml.
admin_psql() { psql -h 127.0.0.1 -p "${PGDOG_PORT}" -U admin -d admin "$@"; }

# Per-database psql helpers (PGPASSWORD already exported).
src_psql()    { psql -h 127.0.0.1 -p 15432 -U pgdog pgdog  "$@"; }
shard0_psql() { psql -h 127.0.0.1 -p 15433 -U pgdog pgdog1 "$@"; }
shard1_psql() { psql -h 127.0.0.1 -p 15434 -U pgdog pgdog2 "$@"; }

src_count()    { src_psql    -tAc "SELECT COUNT(*) FROM $1"; }
shard0_count() { shard0_psql -tAc "SELECT COUNT(*) FROM $1"; }
shard1_count() { shard1_psql -tAc "SELECT COUNT(*) FROM $1"; }

# Pass a psql helper as $1; checks whether the canary row is present on that node.
has_canary() { local fn=$1; "${fn}" -tAc "SELECT 1 FROM copy_data.settings WHERE setting_name='${CANARY}' LIMIT 1" 2>/dev/null | grep -q 1; }

pushd "${COMPOSE_DIR}"

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

# Reset destination shards, then seed the source.
shard0_psql -c "DROP SCHEMA IF EXISTS copy_data CASCADE;"
shard1_psql -c "DROP SCHEMA IF EXISTS copy_data CASCADE;"
src_psql -f "${SCRIPT_DIR}/init.sql"

# Create tables on destination shards via the CLI (no server required).
"${PGDOG_BIN}" --config "${PGDOG_CONFIG}" --users "${USERS_CONFIG}" \
    schema-sync --from-database source --to-database destination --publication pgdog

echo "[retry_test] Starting pgdog server on port ${PGDOG_PORT}..."
PGDOG_PORT="${PGDOG_PORT}" "${PGDOG_BIN}" --config "${PGDOG_CONFIG}" --users "${USERS_CONFIG}" &
PGDOG_PID=$!

READY_ATTEMPTS=0
until admin_psql -c 'SHOW VERSION' >/dev/null 2>&1; do
    READY_ATTEMPTS=$((READY_ATTEMPTS + 1))
    if [ "${READY_ATTEMPTS}" -ge 60 ]; then
        echo "[retry_test] FAIL: pgdog admin not reachable after 30s"
        exit 1
    fi
    if ! kill -0 "${PGDOG_PID}" 2>/dev/null; then
        echo "[retry_test] FAIL: pgdog server exited before admin became reachable"
        exit 1
    fi
    sleep 0.5
done
echo "[retry_test] pgdog admin is ready."

# Start the copy; returns immediately, work runs in background.
admin_psql -c "COPY_DATA source destination pgdog"

# A temporary replication slot on the source means copying is active; kill now to inject a mid-copy failure.
echo "[retry_test] Waiting for COPY_DATA to start copying..."
WAIT_ATTEMPTS=0
until src_psql -tAc "SELECT 1 FROM pg_replication_slots WHERE temporary = true LIMIT 1" 2>/dev/null | grep -q 1; do
    WAIT_ATTEMPTS=$((WAIT_ATTEMPTS + 1))
    if [ "${WAIT_ATTEMPTS}" -ge 200 ]; then
        echo "[retry_test] FAIL: COPY_DATA never created a replication slot after $((WAIT_ATTEMPTS / 20))s"
        exit 1
    fi
    if ! kill -0 "${PGDOG_PID}" 2>/dev/null; then
        echo "[retry_test] FAIL: pgdog server exited before copy started"
        exit 1
    fi
    sleep 0.05
done

# SIGKILL skips docker's 10s grace period, ensuring the kill lands before the in-flight COPY finishes.
echo "[retry_test] Killing shard_1 during active copy..."
docker compose kill shard_1

# RELOAD while shard_1 is down races with the active copy retry.
echo "[retry_test] Issuing RELOAD while shard_1 is down..."
admin_psql -c 'RELOAD'

# Insert the canary only after replication is confirmed live (Assertion 1).
# The copy runs under a REPEATABLE READ snapshot taken at slot creation, so
# rows inserted after that point are invisible to the copy regardless of timing.
# Inserting here rather than earlier is a deliberate choice: it makes the test
# assertion unambiguous — any canary arrival must have come via the replication
# stream, not copied rows.

# Let the retry loop run a few cycles before restoring shard_1.
sleep 2

echo "[retry_test] Starting shard_1..."
docker compose start shard_1

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

# Assertion 1: replication started after the pool reload.
echo "[retry_test] Waiting for SHOW TASKS to report a Replication task..."
REPLICATION_TASK_SEEN=0
for _ in $(seq 1 "${REPLICATION_TIMEOUT}"); do
    if admin_psql -tAc 'SHOW TASKS' 2>/dev/null | grep -q '|replication|'; then
        REPLICATION_TASK_SEEN=1
        break
    fi
    if ! kill -0 "${PGDOG_PID}" 2>/dev/null; then
        echo "[retry_test] FAIL: pgdog server exited before replication started"
        exit 1
    fi
    sleep 1
done

if [ "${REPLICATION_TASK_SEEN}" -ne 1 ]; then
    echo "[retry_test] FAIL: no Replication task within ${REPLICATION_TIMEOUT}s after shard_1 was restored"
    echo "[retry_test]   replication did not start after the copy completed"
    echo "[retry_test] --- SHOW TASKS ---"
    admin_psql -c 'SHOW TASKS' || true
    echo "[retry_test] --- SHOW REPLICATION_SLOTS ---"
    admin_psql -c 'SHOW REPLICATION_SLOTS' || true
    exit 1
fi
echo "[retry_test] OK: Replication task is live."

# Assertion 2: writes made after the copy reach both destination shards via replication.
CANARY="canary_$(date +%s)_$$"
echo "[retry_test] Inserting canary ${CANARY} into source..."
src_psql -c "INSERT INTO copy_data.settings (setting_name, setting_value) VALUES ('${CANARY}', 'canary');"

echo "[retry_test] Waiting for canary to reach both destinations via replication..."
CANARY_DELIVERED=0
for _ in $(seq 1 "${REPLICATION_TIMEOUT}"); do
    if has_canary shard0_psql && has_canary shard1_psql; then
        CANARY_DELIVERED=1
        break
    fi
    if ! kill -0 "${PGDOG_PID}" 2>/dev/null; then
        echo "[retry_test] FAIL: pgdog server exited while waiting for canary"
        exit 1
    fi
    sleep 1
done

if [ "${CANARY_DELIVERED}" -ne 1 ]; then
    echo "[retry_test] FAIL: canary ${CANARY} not replicated within ${REPLICATION_TIMEOUT}s"
    echo "[retry_test]   Replication task was running but the stream did not deliver"
    admin_psql -c 'SHOW REPLICATION_SLOTS' || true
    exit 1
fi
echo "[retry_test] OK: canary replicated to both shards."

# Verify row counts.
# Sharded tables: sum across both destination shards must equal source.
SHARDED_TABLES="copy_data.users copy_data.orders copy_data.order_items copy_data.log_actions copy_data.with_identity"
# Omni tables: each shard must have the full source row count.
OMNI_TABLES="copy_data.countries copy_data.currencies copy_data.categories copy_data.settings"

FAILED=0

for TABLE in ${SHARDED_TABLES}; do
    SRC=$(src_count    "${TABLE}")
    DST0=$(shard0_count "${TABLE}")
    DST1=$(shard1_count "${TABLE}")
    DST=$((DST0 + DST1))
    if [ "${SRC}" -ne "${DST}" ]; then
        echo "[retry_test] MISMATCH ${TABLE}: source=${SRC} total_dest=${DST} (shard0=${DST0} shard1=${DST1})"
        FAILED=1
    else
        echo "[retry_test] OK ${TABLE}: ${SRC} rows"
    fi
done

for TABLE in ${OMNI_TABLES}; do
    SRC=$(src_count    "${TABLE}")
    DST0=$(shard0_count "${TABLE}")
    DST1=$(shard1_count "${TABLE}")
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

echo "[retry_test] PASS: COPY_DATA survived shard outage + pool reload; replication delivered canary."

# Stop pgdog cleanly before tearing down compose.
kill "${PGDOG_PID}" 2>/dev/null || true
wait "${PGDOG_PID}" 2>/dev/null || true
PGDOG_PID=""

docker compose down
popd
