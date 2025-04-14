#!/bin/bash
set -ex
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export PGPASSWORD=pgdog
export PGPORT=6432
export PGHOST=127.0.0.1


${SCRIPT_DIR}/../../../target/release/pgdog \
    --config ${SCRIPT_DIR}/pgdog-enabled.toml \
    --users ${SCRIPT_DIR}/users.toml &
sleep 1

if ! psql -U pgdog1 pgdog -c 'SELECT 1' > /dev/null; then
    echo "AutoDB not working"
    exit 1
fi

psql -U pgdog pgdog -c 'SELECT 1' > /dev/null

killall -TERM pgdog

${SCRIPT_DIR}/../../../target/release/pgdog \
    --config ${SCRIPT_DIR}/pgdog-disabled.toml \
    --users ${SCRIPT_DIR}/users.toml &
sleep 1

if psql -U pgdog1 pgdog -c 'SELECT 1' 2> /dev/null; then
    echo "AutoDB should be disabled"
    exit 1
fi

psql -U pgdog pgdog -c 'SELECT 1' > /dev/null

killall -TERM pgdog
