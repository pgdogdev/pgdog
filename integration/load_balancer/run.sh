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
REPO_ROOT=$(pwd)
popd

PGDOG_BIN_PATH="${PGDOG_BIN:-}"
if [ -z "${PGDOG_BIN_PATH}" ]; then
    pushd ${REPO_ROOT}
    cargo build --release
    PGDOG_BIN_PATH="$(pwd)/target/release/pgdog"
    popd
fi

sleep 2

"${PGDOG_BIN_PATH}" \
    --config ${SCRIPT_DIR}/pgdog.toml \
    --users ${SCRIPT_DIR}/users.toml &

export PGPORT=6432
while ! pg_isready; do
    sleep 1
done

pushd ${SCRIPT_DIR}/pgx
go get
go test -v -count 3
popd

killall pgdog

docker-compose down
