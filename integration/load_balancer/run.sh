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

for p in 5433 5434 5434; do
    export PGPORT=${p}
    while ! pg_isready; do
        sleep 1
    done
done

sleep 2

bash pgdog.sh &

sleep 2

pushd ${SCRIPT_DIR}/pgx
go get
go test -v
popd

killall pgdog

docker-compose down
