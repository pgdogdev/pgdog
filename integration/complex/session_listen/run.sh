#!/bin/bash
#
# Regression test: LISTEN must not be rejected with "pub/sub disabled"
# when pub_sub_channel_size = 0 and the user is in session mode.
#
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

export PGPASSWORD=pgdog

PGDOG_BIN_PATH="${PGDOG_BIN:-${SCRIPT_DIR}/../../../target/release/pgdog}"

killall -TERM pgdog 2> /dev/null || true

"${PGDOG_BIN_PATH}" \
    --config ${SCRIPT_DIR}/pgdog.toml \
    --users ${SCRIPT_DIR}/users.toml &
PGDOG_PID=$!

until pg_isready -h 127.0.0.1 -p 6432 -U pgdog_session -d pgdog; do
    sleep 1
done

psql -h 127.0.0.1 -p 6432 -U pgdog_session -d pgdog -c "LISTEN test_channel"
psql -h 127.0.0.1 -p 6432 -U pgdog_session -d pgdog -c "UNLISTEN *"

echo "PASS: session mode LISTEN/UNLISTEN with pub_sub disabled"

killall -TERM pgdog
wait "${PGDOG_PID}" 2> /dev/null || true
