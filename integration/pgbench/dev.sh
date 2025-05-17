#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
# set -e
export PGPASSWORD=pgdog

pgbench -i -h 127.0.0.1 -U pgdog -p 6432 pgdog
pgbench -i -h 127.0.0.1 -U pgdog -p 6432 pgdog_sharded

pgbench -h 127.0.0.1 -U pgdog -p 6432 pgdog -t 1000 -c 10 --protocol simple -P 1
pgbench -h 127.0.0.1 -U pgdog -p 6432 pgdog -t 1000 -c 10 --protocol extended -P 1
pgbench -h 127.0.0.1 -U pgdog -p 6432 pgdog -t 1000 -c 10 --protocol prepared -P 1

pushd ${SCRIPT_DIR}

pgbench -h 127.0.0.1 -U pgdog -p 6432 pgdog_sharded -t 1000 -c 10 --protocol simple -f sharded.sql -P 1
pgbench -h 127.0.0.1 -U pgdog -p 6432 pgdog_sharded -t 1000 -c 10 --protocol extended -f sharded.sql -P 1
pgbench -h 127.0.0.1 -U pgdog -p 6432 pgdog_sharded -t 1000 -c 10 --protocol prepared -f sharded.sql -P 1

popd
