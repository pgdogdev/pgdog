#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export PGUSER=pgdog
export PGPASSWORD=pgdog
export PGDATABASE=pgdog
export PGHOST=127.0.0.1
export PGPORT=6432

pgbench -i
pgbench -c 10 -j 2 -t 10000000 -f ${SCRIPT_DIR}/../select_1.sql -P 1
