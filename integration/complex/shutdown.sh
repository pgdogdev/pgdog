#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

run_pgdog
wait_for_pgdog

active_venv

pushd ${SCRIPT_DIR}
python shutdown.py pgdog
popd

sleep 1

if pgrep pgdog; then
    echo "Shutdown failed"
    exit 1
fi

run_pgdog
wait_for_pgdog

pushd ${SCRIPT_DIR}
python shutdown.py pgdog_sharded
popd

sleep 1

if pgrep pgdog; then
    echo "Shutdown failed"
    exit 1
fi

stop_pgdog
