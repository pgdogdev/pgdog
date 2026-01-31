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

docker-compose down 2>/dev/null || true

for p in 45000 45001 45002; do
    container=$(docker ps -q --filter "publish=${p}")
    if [ -n "${container}" ]; then
        echo "Stopping docker container on port ${p}: ${container}"
        docker kill ${container} 2>/dev/null || true
    fi
    if pid=$(lsof -t -i:${p} 2>/dev/null); then
        echo "Killing process(es) on port ${p}: ${pid}"
        kill -9 ${pid} 2>/dev/null || true
    fi
done

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
