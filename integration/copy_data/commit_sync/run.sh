#!/bin/bash
# COPY_DATA two-phase commit test (3 shards).
#
# data-sync commits each table copy on all shards with 2PC. The test cuts the link to
# shard 2 at three points of the commit, then checks that every shard holds its rows
# exactly once and that no prepared transaction is left:
#
#   A. before CopyDone:            nothing is prepared, data-sync retries
#   B. in PREPARE TRANSACTION:     the 2PC manager rolls back, data-sync retries
#   C. in COMMIT PREPARED:         the 2PC manager commits, data-sync must not copy again
#
# The shards are databases shard_0/1/2 on the local Postgres, each behind its own
# toxiproxy listener (15500/15501/15502). The source has 2000 rows for each shard.
#
# Requires: integration/setup.sh, integration/toxiproxy-{server,cli}, target/debug/pgdog.
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
INTEGRATION_DIR=$( cd -- "${SCRIPT_DIR}/../.." &> /dev/null && pwd )
PGDOG_BIN=${PGDOG_BIN:-"${SCRIPT_DIR}/../../../target/debug/pgdog"}
PGDOG_CONFIG=${PGDOG_CONFIG:-"${SCRIPT_DIR}/pgdog.toml"}
USERS_CONFIG="${SCRIPT_DIR}/users.toml"
TOXI_CLI="${INTEGRATION_DIR}/toxiproxy-cli"
TOXI_SERVER="${INTEGRATION_DIR}/toxiproxy-server"
WAIT_BEFORE_HEAL=${WAIT_BEFORE_HEAL:-5}

export PGHOST=127.0.0.1 PGPORT=5432 PGUSER=pgdog PGPASSWORD=pgdog

SLOT="commit_sync_slot"
PROXY="commit_sync_shard_2"
NODE_ID="commit-sync"
SYNC_LOG=$(mktemp)
SYNC_PID=""

psql_db() { local db=$1; shift; psql -d "${db}" -qX "$@"; }
count()   { psql_db "$1" -tAc "SELECT count(*) FROM commit_sync.items"; }

drop_slots() {
    psql_db pgdog -tAc "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE '${SLOT}%' OR temporary" >/dev/null 2>&1 || true
}

cleanup() {
    [ -n "${SYNC_PID}" ] && kill "${SYNC_PID}" 2>/dev/null || true
    killall toxiproxy-server 2>/dev/null || true
    psql_db pgdog -tAc "SELECT gid FROM pg_prepared_xacts WHERE database LIKE 'shard_%'" 2>/dev/null |
        while read -r gid; do psql_db pgdog -c "ROLLBACK PREPARED '${gid}'" >/dev/null 2>&1 || true; done
    drop_slots
    for db in pgdog shard_0 shard_1 shard_2; do
        psql_db "${db}" -f "${SCRIPT_DIR}/cleanup.sql" >/dev/null 2>&1 || true
    done
    rm -f "${SYNC_LOG}"
}
trap cleanup EXIT

fail() {
    echo "[commit_sync] FAIL: $1"
    grep -E 'WARN|ERROR|\[2pc\]' "${SYNC_LOG}" || true
    exit 1
}

# Load the source and create empty destination tables.
psql_db pgdog -f "${SCRIPT_DIR}/setup.sql"
for db in shard_0 shard_1 shard_2; do
    psql_db "${db}" -f "${SCRIPT_DIR}/setup_schema.sql"
done

killall toxiproxy-server 2>/dev/null || true
"${TOXI_SERVER}" >/dev/null 2>&1 &
until "${TOXI_CLI}" list >/dev/null 2>&1; do sleep 0.2; done
"${TOXI_CLI}" create --listen 127.0.0.1:15500 --upstream 127.0.0.1:5432 commit_sync_shard_0
"${TOXI_CLI}" create --listen 127.0.0.1:15501 --upstream 127.0.0.1:5432 commit_sync_shard_1
"${TOXI_CLI}" create --listen 127.0.0.1:15502 --upstream 127.0.0.1:5432 "${PROXY}"

SRC=$(count pgdog)
ROWS=$(psql_db pgdog -tAc "SELECT count(*) FROM commit_sync.items WHERE tenant_id = 1")

# A limit_data toxic passes N upstream bytes on each new connection, then closes it.
# Every copy attempt opens a new connection, so every attempt is cut at the same byte.
# Bytes that pgdog sends on the shard 2 copy connection (tls_verify = "disabled"):
#
#   463              startup, SCRAM auth, SET session_replication_role, BEGIN, COPY
#   24 + rows * 547  binary COPY data: header, rows (512-byte payload), trailer
#   5                CopyDone
#   28 + txn         PREPARE TRANSACTION '<txn>'
#   24 + txn         COMMIT PREPARED '<txn>'
#
# <txn> is __pgdog_2pc_<NODE_ID>_<u64>_2 and the u64 has at most 20 digits. A shorter
# u64 lets a few more bytes of COMMIT PREPARED pass, but never the whole message.
DATA_END=$(( 463 + 24 + ROWS * 547 + 7 ))
TXN_MAX=$(( 12 + ${#NODE_ID} + 1 + 20 + 2 ))
CUT_A=${DATA_END}
CUT_B=$(( DATA_END + 5 + 1 ))
CUT_C=$(( DATA_END + 5 + 28 + TXN_MAX + 1 ))

# Usage: scenario NAME LIMIT EXPECTED_LOG
# Run data-sync with the toxic on shard 2. Remove the toxic after WAIT_BEFORE_HEAL
# seconds, or earlier if data-sync exits, so that a retry can pass.
scenario() {
    local name=$1 limit=$2 expected_log=$3 rc=0
    echo "[commit_sync] ${name}: cut shard 2 after ${limit} bytes"

    for db in shard_0 shard_1 shard_2; do
        psql_db "${db}" -c "TRUNCATE commit_sync.items"
    done
    drop_slots

    "${TOXI_CLI}" toxic add --toxicName cut --type limit_data --upstream --attribute bytes="${limit}" "${PROXY}" >/dev/null
    env -u DEPLOYMENT_ID NODE_ID="${NODE_ID}" "${PGDOG_BIN}" --config "${PGDOG_CONFIG}" --users "${USERS_CONFIG}" \
        data-sync --sync-only --skip-schema-sync --from-database source --to-database destination \
        --publication commit_sync --replication-slot "${SLOT}" >"${SYNC_LOG}" 2>&1 &
    SYNC_PID=$!

    for _ in $(seq 1 $(( WAIT_BEFORE_HEAL * 10 ))); do
        kill -0 "${SYNC_PID}" 2>/dev/null || break
        sleep 0.1
    done
    "${TOXI_CLI}" toxic delete --toxicName cut "${PROXY}" >/dev/null
    wait "${SYNC_PID}" || rc=$?
    SYNC_PID=""

    local d0 d1 d2 prepared
    d0=$(count shard_0); d1=$(count shard_1); d2=$(count shard_2)
    prepared=$(psql_db pgdog -tAc "SELECT count(*) FROM pg_prepared_xacts WHERE database LIKE 'shard_%'")
    echo "[commit_sync] ${name}: rc=${rc}, rows ${d0}/${d1}/${d2} (expected ${ROWS} each), prepared=${prepared}"

    [ "${rc}" -eq 0 ] || fail "${name}: data-sync exited with rc=${rc}"
    [ "${d0}" -eq "${ROWS}" ] && [ "${d1}" -eq "${ROWS}" ] && [ "${d2}" -eq "${ROWS}" ] &&
        [ $(( d0 + d1 + d2 )) -eq "${SRC}" ] || fail "${name}: wrong row counts"
    [ "${prepared}" -eq 0 ] || fail "${name}: prepared transactions left on the shards"
    grep -qF "${expected_log}" "${SYNC_LOG}" || fail "${name}: no \"${expected_log}\" in the log; the cut missed its target"
    echo "[commit_sync] OK: ${name}"
}

scenario "A (before CopyDone)" "${CUT_A}" "failed (attempt 1/"
scenario "B (PREPARE TRANSACTION)" "${CUT_B}" "[2pc] manager rolling back transaction"
scenario "C (COMMIT PREPARED)" "${CUT_C}" "[2pc] manager committing transaction"

echo "[commit_sync] PASS"
