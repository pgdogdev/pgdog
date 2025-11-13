#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEFAULT_BIN="${SCRIPT_DIR}/../../target/release/pgdog"
PGDOG_BIN=${PGDOG_BIN:-$DEFAULT_BIN}

export PGUSER=pgdog
export PGDATABASE=pgdog
export PGHOST=127.0.0.1
export PGPORT=5432
export PGPASSWORD=pgdog

pushd ${SCRIPT_DIR}

psql -f init.sql

${PGDOG_BIN} schema-sync --from-database source --to-database destination --publication pgdog
${PGDOG_BIN} data-sync --sync-only --from-database source --to-database destination --publication pgdog --replication-slot copy_data
popd
