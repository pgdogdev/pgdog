#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

run_pgdog
wait_for_pgdog

export PGPASSWORD=pgdog

pgbench -i -h 127.0.0.1 -U pgdog -p 6432 pgdog
pgbench -i -h 127.0.0.1 -U pgdog -p 6432 pgdog_sharded

pgbench -h 127.0.0.1 -U pgdog -p 6432 pgdog -t 1000 -c 10 --protocol simple
pgbench -h 127.0.0.1 -U pgdog -p 6432 pgdog -t 1000 -c 10 --protocol extended
pgbench -h 127.0.0.1 -U pgdog -p 6432 pgdog -t 1000 -c 10 --protocol prepared

pushd ${SCRIPT_DIR}

pgbench -h 127.0.0.1 -U pgdog -p 6432 pgdog_sharded -t 1000 -c 10 --protocol simple -f sharded.sql
pgbench -h 127.0.0.1 -U pgdog -p 6432 pgdog_sharded -t 1000 -c 10 --protocol extended -f sharded.sql
pgbench -h 127.0.0.1 -U pgdog -p 6432 pgdog_sharded -t 1000 -c 10 --protocol prepared -f sharded.sql

popd

stop_pgdog
