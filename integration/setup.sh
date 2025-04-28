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


for db in pgdog shard_0 shard_1; do
    psql -c "CREATE DATABASE $db" || true
    for user in pgdog pgdog1 pgdog2 pgdog3; do
        psql -c "GRANT ALL ON DATABASE $db TO ${user}"
        psql -c "GRANT ALL ON SCHEMA public TO ${user}" ${db}
    done
done

for db in pgdog shard_0 shard_1; do
    for user in pgdog ${USER}; do
        for table in sharded sharded_omni; do
             psql -c "DROP TABLE IF EXISTS ${table}" ${db} -U ${user}
             psql -c "CREATE TABLE IF NOT EXISTS ${table} (id BIGINT PRIMARY KEY, value TEXT)" ${db} -U ${user}
        done
        psql -f ${SCRIPT_DIR}/../pgdog/src/backend/schema/setup.sql ${db} -U ${user}
    done
done

pushd ${SCRIPT_DIR}

set -e

for bin in toxiproxy-server toxiproxy-cli; do
    if [[ ! -f ${bin} ]]; then
        curl -L https://github.com/Shopify/toxiproxy/releases/download/v2.12.0/${bin}-${OS}-${ARCH} > ${bin}
        chmod +x ${bin}
    fi
done

popd
