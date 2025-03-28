#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(arch)

psql -c "CREATE USER pgdog LOGIN SUPERUSER PASSWORD 'pgdog'"

for db in pgdog shard_0 shard_1; do
    psql -c "CREATE DATABASE $db"
    psql -c "GRANT ALL ON DATABASE $db TO pgdog"
    psql -c "GRANT ALL ON SCHEMA public TO pgdog" ${db}
done

for db in shard_0 shard_1; do
    psql -c 'CREATE TABLE IF NOT EXISTS sharded (id BIGINT, value TEXT)' ${db}
done

pushd ${SCRIPT_DIR}

set -e

for bin in toxiproxy-server toxiproxy-cli; do
    if [[ ! -f ${bin} ]]; then
        curl -L https://github.com/Shopify/toxiproxy/releases/download/v2.12.0/${bin}-${OS}-${ARCH} > ${bin}
        chmod +x ${bin}
    fi
done

killall -TERM toxiproxy || true
./toxiproxy-server > toxi.log &

./toxiproxy-cli create --listen :5433 --upstream :5432 primary
./toxiproxy-cli create --listen :5434 --upstream :5432 replica_1
./toxiproxy-cli create --listen :5435 --upstream :5432 replica_2

popd
