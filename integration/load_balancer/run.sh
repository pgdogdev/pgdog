#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

pushd ${SCRIPT_DIR}

export PGUSER=postgres
export PGHOST=127.0.0.1
export PGDATABASE=postgres
export PGPASSWORD=postgres

echo "[load_balancer] Using PGDOG_BIN=${PGDOG_BIN}"
echo "[load_balancer] LLVM_PROFILE_FILE=${LLVM_PROFILE_FILE}"

docker-compose up -d

echo "Waiting for Postgres to be ready"
for p in 45000 45001 45002; do
    export PGPORT=${p}
    while ! pg_isready; do
        sleep 1
    done
done

run_pgdog ${SCRIPT_DIR}

export PGPORT=6432
while ! pg_isready; do
    sleep 1
done

pushd ${SCRIPT_DIR}/pgx
go get
go test -v -count 3
popd

stop_pgdog

docker-compose down
popd
