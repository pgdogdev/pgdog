#!/bin/bash
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEFAULT_BIN="${SCRIPT_DIR}/../../../target/debug/pgdog"
PGDOG_BIN=${PGDOG_BIN:-$DEFAULT_BIN}
PGDOG_CONFIG="${SCRIPT_DIR}/pgdog.toml"
PGDOG_USERS="${SCRIPT_DIR}/users.toml"

export PGPASSWORD=pgdog
export PGUSER=pgdog
export PGHOST=127.0.0.1
export PGPORT=5432
export PGDATABASE=postgres

SOURCE_DATABASE=pgdog
DESTINATION_SHARDS=(pgdog1 pgdog2)
SECOND_DESTINATION_SHARDS=(shard_0 shard_1)

for db in "${DESTINATION_SHARDS[@]}"; do
    dropdb --if-exists "${db}"
    createdb "${db}"
done

for db in "${SOURCE_DATABASE}" "${SECOND_DESTINATION_SHARDS[@]}"; do
    psql -v ON_ERROR_STOP=1 -q -d "${db}" -c "DROP SCHEMA IF EXISTS copy_data CASCADE"
done

psql -v ON_ERROR_STOP=1 -q -f "${SCRIPT_DIR}/../setup.sql" "${SOURCE_DATABASE}"

"${PGDOG_BIN}" --config "${PGDOG_CONFIG}" --users "${PGDOG_USERS}" \
    schema-sync --from-database source --to-database destination --publication pgdog

for db in "${DESTINATION_SHARDS[@]}"; do
    psql -v ON_ERROR_STOP=1 -q -d "${db}" \
        -c "CREATE UNIQUE INDEX IF NOT EXISTS event_types_code_idx ON copy_data.event_types (code)"
done
