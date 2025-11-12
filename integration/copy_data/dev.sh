#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PGDOG_BIN=${SCRIPT_DIR}/../../target/release/pgdog

pushd ${SCRIPT_DIR}

PGUSER=pgdog PGPASSWORD=pgdog psql -f init.sql

${PGDOG_BIN} schema-sync --from-database source --to-database destination --publication pgdog
${PGDOG_BIN} data-sync --sync-only --from-database source --to-database destination --publication pgdog --replication-slot copy_data
popd
