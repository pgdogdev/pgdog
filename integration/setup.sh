#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
if [[ "$OS" == "darwin" ]]; then
    ARCH=arm64
else
    ARCH=amd64
fi


for user in pgdog pgdog1 pgdog2 pgdog3; do
    psql -c "CREATE USER ${user} LOGIN SUPERUSER PASSWORD 'pgdog'" || true
done

# GitHub fix
if [[ "$USER" == "runner" ]]; then
    psql -c "ALTER USER runner PASSWORD 'pgdog' LOGIN;"
fi

export PGPASSWORD='pgdog'
export PGHOST=127.0.0.1
export PGPORT=5432
#export PGUSER='pgdog'

for db in pgdog shard_0 shard_1; do
    psql -c "CREATE DATABASE $db" || true
    for user in pgdog pgdog1 pgdog2 pgdog3; do
        psql -c "GRANT ALL ON DATABASE $db TO ${user}"
        psql -c "GRANT ALL ON SCHEMA public TO ${user}" ${db}
    done
done

for db in pgdog shard_0 shard_1; do
    for table in sharded sharded_omni; do
            psql -c "DROP TABLE IF EXISTS ${table}" ${db} -U pgdog
            psql -c "CREATE TABLE IF NOT EXISTS ${table} (id BIGINT PRIMARY KEY, value TEXT)" ${db} -U pgdog
    done
    psql -f ${SCRIPT_DIR}/../pgdog/src/backend/schema/setup.sql ${db} -U ${user}
done

pushd ${SCRIPT_DIR}

for bin in toxiproxy-server toxiproxy-cli; do
    if [[ ! -f ${bin} ]]; then
        curl -L https://github.com/Shopify/toxiproxy/releases/download/v2.12.0/${bin}-${OS}-${ARCH} > ${bin}
        chmod +x ${bin}
    fi
done

popd
