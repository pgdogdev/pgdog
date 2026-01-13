#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../../common.sh

APPLICATION_NAME="pgdog_cancel_query"
QUERY="SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = '${APPLICATION_NAME}'"

export PGPASSWORD=pgdog

active_venv

run_pgdog "${SCRIPT_DIR}"
wait_for_pgdog

pushd ${SCRIPT_DIR}
python run.py
popd

attempts=0
until [[ "$(psql -h localhost -U pgdog -tAq -c "${QUERY}")" == "0" ]]; do
    if [[ ${attempts} -ge 5 ]]; then
        echo "Found lingering sessions with application_name='${APPLICATION_NAME}'" >&2
        exit 1
    fi
    attempts=$((attempts + 1))
    sleep 5
done

stop_pgdog
