#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
set -e

pushd ${SCRIPT_DIR}

export PGUSER=postgres

export PGHOST=127.0.0.1
export PGDATABASE=postgres
export PGPASSWORD=postgres

docker-compose up -d


echo "Waiting for Postgres to be ready"

for p in 45000 45001 45002; do
    export PGPORT=${p}
    while ! pg_isready; do
        sleep 1
    done
done


pushd ${SCRIPT_DIR}/../../
cargo build --release
popd

sleep 2

cargo run --release -- \
    --config ${SCRIPT_DIR}/pgdog.toml \
    --users ${SCRIPT_DIR}/users.toml &

pushd ${SCRIPT_DIR}/pgx
go get
go test -v
popd

killall pgdog

docker-compose down
