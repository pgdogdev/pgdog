#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
if [[ "$OS" == "darwin" ]]; then
    ARCH=arm64
else
    ARCH=amd64
fi

psql -c "CREATE USER pgdog LOGIN SUPERUSER PASSWORD 'pgdog'"

for db in pgdog shard_0 shard_1; do
    psql -c "CREATE DATABASE $db"
    psql -c "GRANT ALL ON DATABASE $db TO pgdog"
    psql -c "GRANT ALL ON SCHEMA public TO pgdog" ${db}
done

for db in shard_0 shard_1; do
    psql -c 'CREATE TABLE IF NOT EXISTS sharded (id BIGINT, value TEXT)' ${db}
    psql -f ${SCRIPT_DIR}/../pgdog/src/backend/schema/setup.sql ${db}
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
