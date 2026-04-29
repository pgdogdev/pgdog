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

# SQL query constants reused across source capture, poll loop, and final validation.
SQL_POSTS_UPDATED="SELECT COUNT(*) FROM copy_data.posts WHERE title LIKE '%_updated'"
SQL_BODY_SUM="SELECT COALESCE(SUM(octet_length(body)),0) FROM copy_data.posts"
SQL_FULL_EVENTS_COUNT="SELECT COUNT(*) FROM copy_data.full_identity_events"
SQL_FULL_EVENTS_UPDATED="SELECT COUNT(*) FROM copy_data.full_identity_events WHERE label LIKE 'updated_%'"
SQL_EVENT_TYPE_CLICK_LABEL="SELECT label FROM copy_data.event_types WHERE code = 'click'"
SQL_NULL_DESC_LABEL="SELECT label FROM copy_data.event_types WHERE code = 'null_desc'"

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
    psql -d "$db" -tAc "$sql" | tr -d '[:space:]'
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
# each destination shard before data-sync so has_unique_index() finds it.
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
# seq 1..50 → UPDATE (label 'event_N' → 'updated_N'); seq 51..100 → DELETE.
# The WAL record carries all columns in the OLD tuple; the subscriber builds the
# WHERE clause from those values using IS NOT DISTINCT FROM.
psql -d "${SRC_DB}" -c "UPDATE copy_data.full_identity_events SET label = 'updated_' || seq WHERE seq BETWEEN 1 AND 50"
psql -d "${SRC_DB}" -c "DELETE FROM copy_data.full_identity_events WHERE seq BETWEEN 51 AND 100"
FULL_EVENTS_EXPECTED=$(query_one "${SRC_DB}" "${SQL_FULL_EVENTS_COUNT}")
FULL_UPDATED_SRC=$(query_one "${SRC_DB}" "${SQL_FULL_EVENTS_UPDATED}")

# REPLICA IDENTITY FULL test: UPDATE on an omni no-PK table with unique index.
# The subscriber uses ON CONFLICT DO NOTHING for INSERT; plain UPDATE for WAL UPDATE.
psql -d "${SRC_DB}" -c "UPDATE copy_data.event_types SET label = 'Click Updated' WHERE code = 'click'"
# IS NOT DISTINCT FROM test: the null_desc row has description = NULL.
# The FULL identity WHERE clause for this UPDATE includes:
# description IS NOT DISTINCT FROM NULL
# A plain = predicate would match zero rows; this UPDATE would silently not propagate.
psql -d "${SRC_DB}" -c "UPDATE copy_data.event_types SET label = 'Null Desc Updated' WHERE code = 'null_desc'"

stop_pgbench

# Poll the destination until all streaming DML changes have propagated.
echo "Waiting for streaming changes to reach destination (timeout 120s)..."
DEADLINE=$((SECONDS + 120))
while true; do
    UPDATED_DST=$(sum_shards "${DST_DB1}" "${DST_DB2}" "${SQL_POSTS_UPDATED}")
    FULL_EVENTS_DST=$(sum_shards "${DST_DB1}" "${DST_DB2}" "${SQL_FULL_EVENTS_COUNT}" -1)
    FULL_UPDATED_DST=$(sum_shards "${DST_DB1}" "${DST_DB2}" "${SQL_FULL_EVENTS_UPDATED}")
    EVENT_TYPE_LABEL=$(psql -d "${DST_DB1}" -tAc "${SQL_EVENT_TYPE_CLICK_LABEL}" 2>/dev/null \
        | tr -d '[:space:]' || echo "")
    NULL_DESC_LABEL=$(psql -d "${DST_DB1}" -tAc "${SQL_NULL_DESC_LABEL}" 2>/dev/null \
        | tr -d '[:space:]' || echo "")
    if [ "${UPDATED_DST}" -ge 50 ] && \
       [ "${FULL_EVENTS_DST}" -eq "${FULL_EVENTS_EXPECTED}" ] && \
       [ "${FULL_UPDATED_DST}" -eq "${FULL_UPDATED_SRC}" ] && \
       [ "${EVENT_TYPE_LABEL}" = "ClickUpdated" ] && \
       [ "${NULL_DESC_LABEL}" = "NullDescUpdated" ]; then
        break
    fi
    if ! kill -0 "${REPL_PID}" 2>/dev/null; then
        echo "ERROR: replication process exited before delivering all changes"
        echo "  posts updated: ${UPDATED_DST}/50"
        echo "  full_identity_events count: ${FULL_EVENTS_DST} (expected ${FULL_EVENTS_EXPECTED})"
        echo "  full_identity_events updated: ${FULL_UPDATED_DST} (expected ${FULL_UPDATED_SRC})"
        echo "  event_types click label: '${EVENT_TYPE_LABEL}' (expected 'ClickUpdated')"
        echo "  event_types null_desc label: '${NULL_DESC_LABEL}' (expected 'NullDescUpdated')"
        exit 1
    fi
    if [ "${SECONDS}" -ge "${DEADLINE}" ]; then
        echo "ERROR: streaming changes did not reach destination within 120s"
        echo "  posts updated: ${UPDATED_DST}/50"
        echo "  full_identity_events count: ${FULL_EVENTS_DST} (expected ${FULL_EVENTS_EXPECTED})"
        echo "  full_identity_events updated: ${FULL_UPDATED_DST} (expected ${FULL_UPDATED_SRC})"
        echo "  event_types click label: '${EVENT_TYPE_LABEL}' (expected 'ClickUpdated')"
        echo "  event_types null_desc label: '${NULL_DESC_LABEL}' (expected 'NullDescUpdated')"
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
BODY_SRC=$(query_one "${SRC_DB}" "${SQL_BODY_SUM}")
BODY_DST=$(sum_shards "${DST2_DB1}" "${DST2_DB2}" "${SQL_BODY_SUM}")
if [ "${BODY_SRC}" -ne "${BODY_DST}" ]; then
    echo "ERROR unchanged-TOAST: source body sum=${BODY_SRC}, destination sum=${BODY_DST}"
    exit 1
fi
echo "OK unchanged-TOAST body preserved: ${BODY_DST} bytes"

# Title update must have propagated through both resharding hops.
UPDATED_DST=$(sum_shards "${DST2_DB1}" "${DST2_DB2}" "${SQL_POSTS_UPDATED}")
if [ "${UPDATED_DST}" -lt 50 ]; then
    echo "ERROR: title update did not propagate (${UPDATED_DST}/50 rows updated on destination)"
    exit 1
fi
echo "OK title propagated: ${UPDATED_DST}/50 rows updated"

# REPLICA IDENTITY FULL: sharded table UPDATE propagated through 2→2 sync.
FULL_UPDATED_2=$(sum_shards "${DST2_DB1}" "${DST2_DB2}" "${SQL_FULL_EVENTS_UPDATED}")
EXPECTED=$(query_one "${SRC_DB}" "${SQL_FULL_EVENTS_UPDATED}")
if [ "${FULL_UPDATED_2}" -ne "${EXPECTED}" ]; then
    echo "ERROR REPLICA IDENTITY FULL UPDATE: expected ${EXPECTED} 'updated_*' rows, got ${FULL_UPDATED_2} on destination2"
    exit 1
fi
echo "OK REPLICA IDENTITY FULL sharded UPDATE: ${FULL_UPDATED_2} rows propagated"

# REPLICA IDENTITY FULL: sharded table DELETE propagated — total count matches.
FULL_TOTAL_2=$(sum_shards "${DST2_DB1}" "${DST2_DB2}" "${SQL_FULL_EVENTS_COUNT}" -1)
EXPECTED=$(query_one "${SRC_DB}" "${SQL_FULL_EVENTS_COUNT}")
if [ "${FULL_TOTAL_2}" -ne "${EXPECTED}" ]; then
    echo "ERROR REPLICA IDENTITY FULL DELETE: expected ${EXPECTED} rows, got ${FULL_TOTAL_2} on destination2"
    exit 1
fi
echo "OK REPLICA IDENTITY FULL sharded DELETE: ${FULL_TOTAL_2} rows on destination2"

# REPLICA IDENTITY FULL: omni table UPDATE propagated to both shards of destination2.
OMNI_LABEL_0=$(query_one "${DST2_DB1}" "${SQL_EVENT_TYPE_CLICK_LABEL}")
OMNI_LABEL_1=$(query_one "${DST2_DB2}" "${SQL_EVENT_TYPE_CLICK_LABEL}")
if [ "${OMNI_LABEL_0}" != "ClickUpdated" ] || [ "${OMNI_LABEL_1}" != "ClickUpdated" ]; then
    echo "ERROR REPLICA IDENTITY FULL omni UPDATE: shard_0='${OMNI_LABEL_0}' shard_1='${OMNI_LABEL_1}' (expected 'ClickUpdated')"
    exit 1
fi
echo "OK REPLICA IDENTITY FULL omni UPDATE: label='Click Updated' on both shards"

# IS NOT DISTINCT FROM: the null_desc row (description IS NULL) must have propagated
# through both resharding hops. A plain = predicate would have matched zero rows on the
# source and left the destination label unchanged.
NULL_LABEL_0=$(query_one "${DST2_DB1}" "${SQL_NULL_DESC_LABEL}")
NULL_LABEL_1=$(query_one "${DST2_DB2}" "${SQL_NULL_DESC_LABEL}")
if [ "${NULL_LABEL_0}" != "NullDescUpdated" ] || [ "${NULL_LABEL_1}" != "NullDescUpdated" ]; then
    echo "ERROR IS NOT DISTINCT FROM: shard_0='${NULL_LABEL_0}' shard_1='${NULL_LABEL_1}' (expected 'NullDescUpdated')"
    exit 1
fi
echo "OK IS NOT DISTINCT FROM: null_desc label='Null Desc Updated' on both shards"

psql -f "${SCRIPT_DIR}/init.sql"

popd
