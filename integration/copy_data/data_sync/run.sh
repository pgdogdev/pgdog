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

SHARDED_TABLES="copy_data.users copy_data.orders copy_data.order_items copy_data.log_actions copy_data.with_identity copy_data.posts"
OMNI_TABLES="copy_data.countries copy_data.currencies copy_data.categories"

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
psql -d pgdog -c "UPDATE copy_data.posts SET title = title || '_updated' WHERE id BETWEEN 1 AND 50"

stop_pgbench

# Poll the destination until the expected data is replicated
echo "Waiting for title update to reach destination (timeout 120s)..."
DEADLINE=$((SECONDS + 120))
while true; do
    UPDATED_DST=$((
        $(psql -d pgdog1 -tAc "SELECT COUNT(*) FROM copy_data.posts WHERE title LIKE '%_updated'" 2>/dev/null || echo 0) +
        $(psql -d pgdog2 -tAc "SELECT COUNT(*) FROM copy_data.posts WHERE title LIKE '%_updated'" 2>/dev/null || echo 0)
    ))
    if [ "${UPDATED_DST}" -ge 50 ]; then
        break
    fi
    if ! kill -0 "${REPL_PID}" 2>/dev/null; then
        echo "ERROR: replication process exited before delivering title update (${UPDATED_DST}/50)"
        exit 1
    fi
    if [ "${SECONDS}" -ge "${DEADLINE}" ]; then
        echo "ERROR: title update did not reach destination within 120s (${UPDATED_DST}/50)"
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

# Check row counts: destination (pgdog1 + pgdog2) vs destination2 (shard_0 + shard_1)
echo "Checking row counts: destination -> destination2..."
for TABLE in ${SHARDED_TABLES}; do
    SRC1=$(psql -d pgdog1 -tAc "SELECT COUNT(*) FROM ${TABLE}")
    SRC2=$(psql -d pgdog2 -tAc "SELECT COUNT(*) FROM ${TABLE}")
    SRC=$((SRC1 + SRC2))
    DST1=$(psql -d shard_0 -tAc "SELECT COUNT(*) FROM ${TABLE}")
    DST2=$(psql -d shard_1 -tAc "SELECT COUNT(*) FROM ${TABLE}")
    DST=$((DST1 + DST2))
    if [ "${SRC}" -ne "${DST}" ]; then
        echo "MISMATCH ${TABLE}: source=${SRC} destination=${DST} (shard0=${DST1} shard1=${DST2})"
        exit 1
    fi
    echo "OK ${TABLE}: ${SRC} rows"
done

for TABLE in ${OMNI_TABLES}; do
    SRC=$(psql -d pgdog1 -tAc "SELECT COUNT(*) FROM ${TABLE}")
    DST1=$(psql -d shard_0 -tAc "SELECT COUNT(*) FROM ${TABLE}")
    DST2=$(psql -d shard_1 -tAc "SELECT COUNT(*) FROM ${TABLE}")
    if [ "${SRC}" -ne "${DST1}" ] || [ "${SRC}" -ne "${DST2}" ]; then
        echo "MISMATCH ${TABLE}: source=${SRC} shard0=${DST1} shard1=${DST2} (expected ${SRC} on each shard)"
        exit 1
    fi
    echo "OK ${TABLE}: ${SRC} rows on each shard"
done

# TOAST invariant: destination body bytes must equal source body bytes.
# If the subscriber wrote an empty string instead of skipping the column,
# the destination sum will be far smaller than the source.
BODY_SRC=$(psql -d pgdog -tAc "SELECT COALESCE(SUM(octet_length(body)),0) FROM copy_data.posts")
BODY_DST1=$(psql -d shard_0 -tAc "SELECT COALESCE(SUM(octet_length(body)),0) FROM copy_data.posts" 2>/dev/null || echo 0)
BODY_DST2=$(psql -d shard_1 -tAc "SELECT COALESCE(SUM(octet_length(body)),0) FROM copy_data.posts" 2>/dev/null || echo 0)
BODY_DST=$((BODY_DST1 + BODY_DST2))
if [ "${BODY_SRC}" -ne "${BODY_DST}" ]; then
    echo "ERROR unchanged-TOAST: source body sum=${BODY_SRC}, destination sum=${BODY_DST}"
    exit 1
fi
echo "OK unchanged-TOAST body preserved: ${BODY_DST} bytes"

# Title update must have propagated through both resharding hops.
UPDATED_DST=$((
    $(psql -d shard_0 -tAc "SELECT COUNT(*) FROM copy_data.posts WHERE title LIKE '%_updated'" 2>/dev/null || echo 0) +
    $(psql -d shard_1 -tAc "SELECT COUNT(*) FROM copy_data.posts WHERE title LIKE '%_updated'" 2>/dev/null || echo 0)
))
if [ "${UPDATED_DST}" -lt 50 ]; then
    echo "ERROR: title update did not propagate (${UPDATED_DST}/50 rows updated on destination)"
    exit 1
fi
echo "OK title propagated: ${UPDATED_DST}/50 rows updated"

psql -f "${SCRIPT_DIR}/init.sql"

popd
