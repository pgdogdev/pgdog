#!/bin/bash
# Integration test: 0→2 and 2→2 resharding with live write traffic.
#
# Requires:
#   - local postgres at port 5432
#   - databases: pgdog, pgdog1, pgdog2, shard_0, shard_1 (created by integration/setup.sh)
#   - max_replication_slots >= 32 in postgresql.conf
#     Each data-sync creates one permanent slot per source shard plus one temporary
#     slot per parallel table copy. With resharding_parallel_copies=5 and a 2-shard
#     source, peak usage is 3 permanent + 2×5 temporary = 13 slots. The default of 10
#     is not enough; set max_replication_slots = 32 in postgresql.conf and reload.
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEFAULT_BIN="${SCRIPT_DIR}/../../../target/debug/pgdog"
PGDOG_BIN=${PGDOG_BIN:-$DEFAULT_BIN}
PGDOG_CONFIG="${SCRIPT_DIR}/pgdog.toml"
PGDOG_USERS="${SCRIPT_DIR}/users.toml"

export PGUSER=pgdog
export PGDATABASE=pgdog
export PGHOST=127.0.0.1
export PGPORT=5432
export PGPASSWORD=pgdog

BENCH_PID=""
REPL_PID=""

cleanup() {
    if [ -n "${BENCH_PID}" ]; then
        kill ${BENCH_PID} 2>/dev/null || true
        wait ${BENCH_PID} 2>/dev/null || true
    fi
    if [ -n "${REPL_PID}" ]; then
        kill ${REPL_PID} 2>/dev/null || true
        wait ${REPL_PID} 2>/dev/null || true
    fi
}
trap cleanup EXIT

start_pgbench() {
    (
        pgbench -h 127.0.0.1 -p 5432 -U pgdog pgdog \
            -t 100000000 -c 3 --protocol extended \
            -f "${SCRIPT_DIR}/pgbench.sql" -P 1

    ) &
    BENCH_PID=$!
}

stop_pgbench() {
    if [ -n "${BENCH_PID}" ]; then
        kill ${BENCH_PID} 2>/dev/null || true
        wait ${BENCH_PID} 2>/dev/null || true
        BENCH_PID=""
    fi
}

# Database name constants for the three roles in this test.
SRC_DB="pgdog"      # single source database
DST_DB1="pgdog1"    # destination shard 0 (0→2 target)
DST_DB2="pgdog2"    # destination shard 1 (0→2 target)
DST2_DB1="shard_0"  # destination2 shard 0 (2→2 target)
DST2_DB2="shard_1"  # destination2 shard 1 (2→2 target)

# sum_shards DB1 DB2 SQL [FALLBACK]
# Runs SQL on DB1 and DB2 independently and returns their integer sum.
# FALLBACK (default 0) is substituted when a query fails, e.g. while a table
# is still being created or when polling before replication catches up.
sum_shards() {
    local db1=$1 db2=$2 sql=$3 fallback=${4:-0}
    echo $((
        $(psql -d "$db1" -tAc "$sql" 2>/dev/null || echo "$fallback") +
        $(psql -d "$db2" -tAc "$sql" 2>/dev/null || echo "$fallback")
    ))
}

# query_one DB SQL
# Runs SQL on a single database and returns the result with whitespace stripped.
# Errors are not suppressed — a failure here aborts the script.
query_one() {
    local db=$1 sql=$2
    psql -d "$db" -tAc "$sql" | tr -d '\n\r'
}

SHARDED_TABLES="copy_data.users copy_data.orders copy_data.order_items copy_data.log_actions copy_data.with_identity copy_data.posts copy_data.full_identity_events"
OMNI_TABLES="copy_data.countries copy_data.currencies copy_data.categories copy_data.event_types"

pushd ${SCRIPT_DIR}

# Teardown: drop stale slots and schemas.
psql -f "${SCRIPT_DIR}/init.sql"
# Setup: populate source database.
psql -f "${SCRIPT_DIR}/../setup.sql"

#
# 0 -> 2
#
${PGDOG_BIN} --config "${PGDOG_CONFIG}" --users "${PGDOG_USERS}" \
    schema-sync --from-database source --to-database destination --publication pgdog
# event_types has REPLICA IDENTITY FULL (omni). The unique index on `code` is
# PostData and is not synced by schema-sync pre-data, so create it explicitly on
# each destination shard before data-sync so tables_missing_unique_index() finds it.
psql -d "${DST_DB1}" -c "CREATE UNIQUE INDEX IF NOT EXISTS event_types_code_idx ON copy_data.event_types (code)"
psql -d "${DST_DB2}" -c "CREATE UNIQUE INDEX IF NOT EXISTS event_types_code_idx ON copy_data.event_types (code)"
start_pgbench
${PGDOG_BIN} --config "${PGDOG_CONFIG}" --users "${PGDOG_USERS}" \
    data-sync --from-database source --to-database destination --publication pgdog &
REPL_PID=$!

# Give replication a moment to connect.
sleep 2

# Check that the replication process is still alive.
if ! kill -0 ${REPL_PID} 2>/dev/null; then
    echo "ERROR: replication process exited early"
    wait ${REPL_PID}
    exit $?
fi

# Let the initial table copy finish before injecting streaming DML.
echo "Letting replication run for 15 seconds..."
sleep 15

# TOAST stream test: rows were seeded in setup.sql and copied to the destination
# during the initial snapshot. Now UPDATE only `title`, leaving `body` untouched.
# PostgreSQL emits a 'u' (unchanged-TOAST) marker for `body` in the WAL record.
# The subscriber must issue a filtered UPDATE that skips `body` entirely;
# if it instead writes an empty string the body sum check below will catch it.
psql -d "${SRC_DB}" -c "UPDATE copy_data.posts SET title = title || '_updated' WHERE id BETWEEN 1 AND 50"

# REPLICA IDENTITY FULL test: UPDATE and DELETE on a sharded no-PK table.
# seq 1..50 → UPDATE (label only; body unchanged → PG emits 'u' for body in WAL).
# seq 51..100 → DELETE.
# WAL shape: OLD = 4 columns inline (toast_flatten_tuple); NEW = 3 present + 1 'u'.
# Subscriber must bind the 3 present columns; binding all 4 from OLD causes 08P01.
psql -d "${SRC_DB}" -c "UPDATE copy_data.full_identity_events SET label = 'updated_' || seq WHERE seq BETWEEN 1 AND 50"
psql -d "${SRC_DB}" -c "DELETE FROM copy_data.full_identity_events WHERE seq BETWEEN 51 AND 100"
SQL_FIE_COUNT="SELECT COUNT(*) FROM copy_data.full_identity_events"
SQL_FIE_UPDATED="SELECT COUNT(*) FROM copy_data.full_identity_events WHERE label LIKE 'updated_%'"
SQL_FIE_BODY="SELECT COALESCE(SUM(octet_length(body)),0) FROM copy_data.full_identity_events"
FULL_EVENTS_EXPECTED=$(query_one "${SRC_DB}" "${SQL_FIE_COUNT}")
FULL_UPDATED_SRC=$(query_one "${SRC_DB}" "${SQL_FIE_UPDATED}")
FULL_EVENTS_BODY_SRC=$(query_one "${SRC_DB}" "${SQL_FIE_BODY}")

# REPLICA IDENTITY FULL test: UPDATE on an omni no-PK table with unique index.
# The subscriber uses ON CONFLICT DO NOTHING for INSERT; plain UPDATE for WAL UPDATE.
psql -d "${SRC_DB}" -c "UPDATE copy_data.event_types SET label = 'Click Updated' WHERE code = 'click'"
# IS NOT DISTINCT FROM test: the null_desc row has description = NULL.
# The FULL identity WHERE clause for this UPDATE includes:
# description IS NOT DISTINCT FROM NULL
# A plain = predicate would match zero rows; this UPDATE would silently not propagate.
psql -d "${SRC_DB}" -c "UPDATE copy_data.event_types SET label = 'Null Desc Updated' WHERE code = 'null_desc'"

# Duplicate-row UPDATE test (ctid single-row targeting).
# Two identical rows (tenant_id=1, seq=200, label='dup_label') were seeded. Update exactly
# one on the source by targeting its ctid. PgDog receives one WAL UPDATE event whose OLD
# tuple matches both destination rows; ctid targeting must touch only one.
psql -d "${SRC_DB}" -c "UPDATE copy_data.full_identity_events SET label = 'dup_changed'
    WHERE ctid = (SELECT ctid FROM copy_data.full_identity_events WHERE seq = 200 AND label = 'dup_label' LIMIT 1)"

# Cross-shard FULL identity UPDATE test (fill_toasted_from + routing from filled tuple).
# Move seq=1 from tenant_id=1 (shard 0) to tenant_id=3 (shard 1).
# hash(1)%2=0, hash(3)%2=1 — verified cross-shard with PgDog's bigint hash.
# body is STORAGE EXTERNAL so the WAL new tuple carries 'u' for body.
# PgDog must fill body from old_full before routing (P1 fix) and before building the INSERT.
psql -d "${SRC_DB}" -c "UPDATE copy_data.full_identity_events SET tenant_id = 3 WHERE seq = 1"

# REPLICATION SENTINEL — must be the last DML issued against the source.
# Updating this row to 'sentinel_done' produces a WAL record that is downstream of
# every preceding change. The poll loop below waits for it to land on the destination.
psql -d "${SRC_DB}" -c "UPDATE copy_data.full_identity_events SET label = 'sentinel_done' WHERE seq = 999"
stop_pgbench

# Wait for the replication sentinel to land on the destination.
# seq=999 is dedicated solely to this purpose — see setup.sql.
# WAL is ordered: once the sentinel row has propagated, every preceding change has too.
echo "Waiting for streaming changes to reach destination (timeout 120s)..."
DEADLINE=$((SECONDS + 120))
while true; do
    SENTINEL=$(sum_shards "${DST_DB1}" "${DST_DB2}" \
        "SELECT COUNT(*) FROM copy_data.full_identity_events WHERE seq = 999 AND label = 'sentinel_done'" 0)
    [ "${SENTINEL}" -eq 1 ] && break
    if ! kill -0 "${REPL_PID}" 2>/dev/null; then
        echo "ERROR: replication process exited before the sentinel (seq=999 label=sentinel_done) was delivered"
        exit 1
    fi
    if [ "${SECONDS}" -ge "${DEADLINE}" ]; then
        echo "ERROR: streaming changes did not reach destination within 120s"
        exit 1
    fi
    sleep 1
done

# Stop replication and capture its exit code.
kill ${REPL_PID} 2>/dev/null || true
set +e
wait ${REPL_PID}
REPL_EXIT=$?
set -e
REPL_PID=""

# 0, 130 (SIGINT), 143 (SIGTERM) are all normal shutdown codes.
if [ ${REPL_EXIT} -ne 0 ] && [ ${REPL_EXIT} -ne 130 ] && [ ${REPL_EXIT} -ne 143 ]; then
    echo "ERROR: replication process exited with code ${REPL_EXIT}"
    exit ${REPL_EXIT}
fi

${PGDOG_BIN} --config "${PGDOG_CONFIG}" --users "${PGDOG_USERS}" \
    schema-sync --from-database source --to-database destination --publication pgdog --cutover

#
# 2 --> 2
#
${PGDOG_BIN} --config "${PGDOG_CONFIG}" --users "${PGDOG_USERS}" \
    schema-sync --from-database destination --to-database destination2 --publication pgdog
${PGDOG_BIN} --config "${PGDOG_CONFIG}" --users "${PGDOG_USERS}" \
    data-sync --sync-only --from-database destination --to-database destination2 --publication pgdog --replication-slot copy_data_2
${PGDOG_BIN} --config "${PGDOG_CONFIG}" --users "${PGDOG_USERS}" \
    schema-sync --from-database destination --to-database destination2 --publication pgdog --cutover

# Check row counts: source (pgdog) vs destination2 (shard_0 + shard_1).
# Both resharding hops must be lossless end-to-end.
echo "Checking row counts: source -> destination2..."
for TABLE in ${SHARDED_TABLES}; do
    SRC=$(query_one "${SRC_DB}" "SELECT COUNT(*) FROM ${TABLE}")
    DST=$(sum_shards "${DST2_DB1}" "${DST2_DB2}" "SELECT COUNT(*) FROM ${TABLE}")
    if [ "${SRC}" -ne "${DST}" ]; then
        echo "MISMATCH ${TABLE}: source=${SRC} destination2=${DST}"
        exit 1
    fi
    echo "OK ${TABLE}: ${SRC} rows"
done

for TABLE in ${OMNI_TABLES}; do
    SRC=$(query_one "${SRC_DB}" "SELECT COUNT(*) FROM ${TABLE}")
    DST1=$(query_one "${DST2_DB1}" "SELECT COUNT(*) FROM ${TABLE}")
    DST2=$(query_one "${DST2_DB2}" "SELECT COUNT(*) FROM ${TABLE}")
    if [ "${SRC}" -ne "${DST1}" ] || [ "${SRC}" -ne "${DST2}" ]; then
        echo "MISMATCH ${TABLE}: source=${SRC} shard0=${DST1} shard1=${DST2} (expected ${SRC} on each shard)"
        exit 1
    fi
    echo "OK ${TABLE}: ${SRC} rows on each shard"
done

# TOAST invariant: destination body bytes must equal source body bytes.
# If the subscriber wrote an empty string instead of skipping the column,
# the destination sum will be far smaller than the source.
sql="SELECT COALESCE(SUM(octet_length(body)),0) FROM copy_data.posts"
BODY_SRC=$(query_one "${SRC_DB}" "${sql}")
BODY_DST=$(sum_shards "${DST2_DB1}" "${DST2_DB2}" "${sql}")
if [ "${BODY_SRC}" -ne "${BODY_DST}" ]; then
    echo "ERROR unchanged-TOAST: source body sum=${BODY_SRC}, destination sum=${BODY_DST}"
    exit 1
fi
echo "OK unchanged-TOAST body preserved: ${BODY_DST} bytes"

# Title update must have propagated through both resharding hops.
UPDATED_DST=$(sum_shards "${DST2_DB1}" "${DST2_DB2}" "SELECT COUNT(*) FROM copy_data.posts WHERE title LIKE '%_updated'")
if [ "${UPDATED_DST}" -lt 50 ]; then
    echo "ERROR: title update did not propagate (${UPDATED_DST}/50 rows updated on destination)"
    exit 1
fi
echo "OK title propagated: ${UPDATED_DST}/50 rows updated"

# REPLICA IDENTITY FULL: sharded table UPDATE propagated through 2→2 sync.
FULL_UPDATED_2=$(sum_shards "${DST2_DB1}" "${DST2_DB2}" "${SQL_FIE_UPDATED}")
EXPECTED=$(query_one "${SRC_DB}" "${SQL_FIE_UPDATED}")
if [ "${FULL_UPDATED_2}" -ne "${EXPECTED}" ]; then
    echo "ERROR REPLICA IDENTITY FULL UPDATE: expected ${EXPECTED} 'updated_*' rows, got ${FULL_UPDATED_2} on destination2"
    exit 1
fi
echo "OK REPLICA IDENTITY FULL sharded UPDATE: ${FULL_UPDATED_2} rows propagated"

# REPLICA IDENTITY FULL: sharded table DELETE propagated — total count matches.
FULL_TOTAL_2=$(sum_shards "${DST2_DB1}" "${DST2_DB2}" "${SQL_FIE_COUNT}" -1)
EXPECTED=$(query_one "${SRC_DB}" "${SQL_FIE_COUNT}")
if [ "${FULL_TOTAL_2}" -ne "${EXPECTED}" ]; then
    echo "ERROR REPLICA IDENTITY FULL DELETE: expected ${EXPECTED} rows, got ${FULL_TOTAL_2} on destination2"
    exit 1
fi
echo "OK REPLICA IDENTITY FULL sharded DELETE: ${FULL_TOTAL_2} rows on destination2"

# REPLICA IDENTITY FULL: omni table UPDATE propagated to both shards of destination2.
sql="SELECT label FROM copy_data.event_types WHERE code = 'click'"
OMNI_LABEL_0=$(query_one "${DST2_DB1}" "${sql}")
OMNI_LABEL_1=$(query_one "${DST2_DB2}" "${sql}")
if [ "${OMNI_LABEL_0}" != "Click Updated" ] || [ "${OMNI_LABEL_1}" != "Click Updated" ]; then
    echo "ERROR REPLICA IDENTITY FULL omni UPDATE: shard_0='${OMNI_LABEL_0}' shard_1='${OMNI_LABEL_1}' (expected 'Click Updated')"
    exit 1
fi
echo "OK REPLICA IDENTITY FULL omni UPDATE: label='Click Updated' on both shards"

# IS NOT DISTINCT FROM: the null_desc row (description IS NULL) must have propagated
# through both resharding hops. A plain = predicate would have matched zero rows on the
# source and left the destination label unchanged.
sql="SELECT label FROM copy_data.event_types WHERE code = 'null_desc'"
NULL_LABEL_0=$(query_one "${DST2_DB1}" "${sql}")
NULL_LABEL_1=$(query_one "${DST2_DB2}" "${sql}")
if [ "${NULL_LABEL_0}" != "Null Desc Updated" ] || [ "${NULL_LABEL_1}" != "Null Desc Updated" ]; then
    echo "ERROR IS NOT DISTINCT FROM: shard_0='${NULL_LABEL_0}' shard_1='${NULL_LABEL_1}' (expected 'Null Desc Updated')"
    exit 1
fi
echo "OK IS NOT DISTINCT FROM: null_desc label='Null Desc Updated' on both shards"
# REPLICA IDENTITY FULL + TOAST slow-path: full_identity_events.body must be preserved.
# The seq 1..50 label UPDATE leaves body unchanged; PG emits 'u' for body in the WAL.
# OLD carries all 4 columns inline (toast_flatten_tuple); NEW carries 3 present + 1 'u'.
# If the subscriber bound all 4 from OLD, PG would have rejected with 08P01 and
# replication would have stalled before the poll completed. If it bound correctly but
# zeroed body in the SET clause, the byte sum collapses here.
FULL_EVENTS_BODY_DST=$(sum_shards "${DST2_DB1}" "${DST2_DB2}" "${SQL_FIE_BODY}")
if [ "${FULL_EVENTS_BODY_DST}" -ne "${FULL_EVENTS_BODY_SRC}" ]; then
    echo "ERROR FULL identity TOAST slow-path: source body sum=${FULL_EVENTS_BODY_SRC}, destination2 sum=${FULL_EVENTS_BODY_DST}"
    echo "  slow-path UPDATE corrupted or dropped the unchanged body column"
    exit 1
fi
echo "OK FULL identity TOAST slow-path: body preserved (${FULL_EVENTS_BODY_DST} bytes)"

# Duplicate-row UPDATE: ctid targeting must have changed exactly one of the two identical rows.
# Both resharding hops must preserve this invariant.
DUP_CHANGED_2=$(sum_shards "${DST2_DB1}" "${DST2_DB2}" "SELECT COUNT(*) FROM copy_data.full_identity_events WHERE seq = 200 AND label = 'dup_changed'")
DUP_TOTAL_2=$(sum_shards "${DST2_DB1}" "${DST2_DB2}" "SELECT COUNT(*) FROM copy_data.full_identity_events WHERE seq = 200")
if [ "${DUP_CHANGED_2}" -ne 1 ] || [ "${DUP_TOTAL_2}" -ne 2 ]; then
    echo "ERROR ctid duplicate-row UPDATE: changed=${DUP_CHANGED_2} (expected 1), total=${DUP_TOTAL_2} (expected 2)"
    echo "  ctid targeting either modified both rows or none"
    exit 1
fi
echo "OK ctid duplicate-row UPDATE: exactly 1 row changed, ${DUP_TOTAL_2} rows total"

# Cross-shard FULL UPDATE: seq=1 moved from tenant_id=1 to tenant_id=3.
# body was STORAGE EXTERNAL, so WAL new tuple carried 'u' for body — exercises fill_toasted_from.
# The row must exist exactly once across both shards after both resharding hops.
XSHARD_TOTAL_2=$(sum_shards "${DST2_DB1}" "${DST2_DB2}" "SELECT COUNT(*) FROM copy_data.full_identity_events WHERE seq = 1")
if [ "${XSHARD_TOTAL_2}" -ne 1 ]; then
    echo "ERROR cross-shard FULL UPDATE: found ${XSHARD_TOTAL_2} copies of seq=1 (expected 1)"
    echo "  row was either duplicated or lost during the shard move"
    exit 1
fi
echo "OK cross-shard FULL UPDATE: seq=1 exists exactly once after shard move"

psql -f "${SCRIPT_DIR}/init.sql"

popd
