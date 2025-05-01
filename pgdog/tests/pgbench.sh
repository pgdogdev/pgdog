#!/bin/bash
#
# pgBench test run.
#
export PGPORT=${1:-6432}
PGPASSWORD=pgdog pgbench -i -h 127.0.0.1 -U pgdog pgdog
PGPASSWORD=pgdog pgbench -P 1 -h 127.0.0.1 -U pgdog pgdog -c 10 -t 100000 -S --protocol extended
