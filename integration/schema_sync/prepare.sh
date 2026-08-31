#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

export PGPASSWORD=pgdog
export PGUSER=pgdog
export PGHOST=127.0.0.1
export PGPORT=5432

SOURCE_SHARDS=(pgdog1_shard_0 pgdog1_shard_1)
DESTINATION_SHARDS=(pgdog2_shard_0 pgdog2_shard_1 pgdog2_shard_2)

for db in "${SOURCE_SHARDS[@]}" "${DESTINATION_SHARDS[@]}"; do
    dropdb --if-exists "${db}"
    createdb "${db}"
done

for db in "${SOURCE_SHARDS[@]}"; do
    psql -v ON_ERROR_STOP=1 -q -f "${SCRIPT_DIR}/ecommerce_schema.sql" "${db}"
    psql -v ON_ERROR_STOP=1 -q -c 'CREATE PUBLICATION pgdog FOR ALL TABLES' "${db}"
done
