#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh
export PGPASSWORD=pgdog

HEADROOM=${HEADROOM:-5}
CONNECTIONS=${CONNECTIONS:-20}
DURATION=${DURATION:-3}
RECOVERY=${RECOVERY:-30}
PGDOG_PORT=6432
HEALTHCHECK_PORT=8080
METRICS_PORT=9090
export PGCONNECT_TIMEOUT=${RECOVERY}

function check_all() {
    if ! psql -h 127.0.0.1 -p ${PGDOG_PORT} -U pgdog pgdog -c 'SELECT 1' > /dev/null; then
        echo "FAIL: pgdog port ${PGDOG_PORT} does not respond $1"
        exit 1
    fi
    if ! curl -fsS -m ${RECOVERY} http://127.0.0.1:${HEALTHCHECK_PORT} | grep -qx up; then
        echo "FAIL: healthcheck port ${HEALTHCHECK_PORT} is not up $1"
        exit 1
    fi
    if ! curl -fsS -m ${RECOVERY} http://127.0.0.1:${METRICS_PORT} | grep -q max_connections; then
        echo "FAIL: metrics port ${METRICS_PORT} do not respond $1"
        exit 1
    fi
}

run_pgdog integration/server
wait_for_pgdog
PID=$(cat ${COMMON_DIR}/pgdog.pid)

check_all "before load"

HIGHEST_FD=$(ls /proc/${PID}/fd | sort -n | tail -n 1)
NOFILE=$((HIGHEST_FD + 1 + HEADROOM))
prlimit --pid "${PID}" --nofile=${NOFILE}:${NOFILE}

python3 ${SCRIPT_DIR}/load.py "${CONNECTIONS}" "${DURATION}" \
    ${PGDOG_PORT} ${HEALTHCHECK_PORT} ${METRICS_PORT}

check_all "after ${CONNECTIONS} connections per port with NOFILE=${NOFILE}"

echo "OK: all listeners respond after ${CONNECTIONS} connections per port with NOFILE=${NOFILE}"
stop_pgdog
