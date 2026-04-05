#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEFAULT_BIN="${SCRIPT_DIR}/../../target/debug/pgdog"
PGDOG_BIN=${PGDOG_BIN:-$DEFAULT_BIN}

export PGUSER=pgdog
export PGDATABASE=pgdog
export PGHOST=127.0.0.1
export PGPORT=5432
export PGPASSWORD=pgdog

cleanup() {
    if [ -n "${REPL_PID}" ]; then
        kill ${REPL_PID} 2>/dev/null || true
        wait ${REPL_PID} 2>/dev/null || true
    fi
}
trap cleanup EXIT

pushd ${SCRIPT_DIR}

psql -f init.sql

#
# 0 -> 2
#
${PGDOG_BIN} schema-sync --from-database source --to-database destination --publication pgdog
${PGDOG_BIN} data-sync --sync-only --from-database source --to-database destination --publication pgdog --replication-slot copy_data
${PGDOG_BIN} schema-sync --from-database source --to-database destination --publication pgdog --cutover

#
# 2 -> 2
#
${PGDOG_BIN} schema-sync --from-database destination --to-database destination2 --publication pgdog
${PGDOG_BIN} data-sync --sync-only --from-database destination --to-database destination2 --publication pgdog --replication-slot copy_data
${PGDOG_BIN} schema-sync --from-database destination --to-database destination2 --publication pgdog --cutover

# Start replication in the background.
${PGDOG_BIN} data-sync --replicate-only --from-database source --to-database destination --publication pgdog &
REPL_PID=$!

# Give replication a moment to connect.
sleep 2

# Check that the replication process is still alive.
if ! kill -0 ${REPL_PID} 2>/dev/null; then
    echo "ERROR: replication process exited early"
    wait ${REPL_PID}
    exit $?
fi

# Run pgbench against the source database — writes land on the source and
# get replicated to the destination shards via logical replication.
pgbench -h 127.0.0.1 -p 5432 -U pgdog pgdog \
    -t 1000 -c 3 --protocol extended \
    -f "${SCRIPT_DIR}/pgbench.sql" -P 1

# Let replication catch up.
sleep 3

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

psql -f init.sql

popd
