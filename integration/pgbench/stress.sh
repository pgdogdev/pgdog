#!/bin/bash
#
# This will create a decent amount of churn on the network & message
# buffers, by generating different size responses.
#
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export PGPASSWORD=pgdog

pushd ${SCRIPT_DIR}
pgbench -h 127.0.0.1 -U pgdog -p 6432 pgdog -t 100 -c 10 --protocol simple -f stress.sql -P 1
popd
