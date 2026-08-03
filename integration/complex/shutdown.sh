#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

# Draining the pools takes as long as it takes, and a busy runner needs more
# than the moment we'd like it to be. Poll instead of guessing.
function wait_for_shutdown() {
    local waited=0
    while pgrep -x pgdog > /dev/null && [ ${waited} -lt 100 ]; do
        sleep 0.1
        waited=$((waited + 1))
    done

    if pgrep -x pgdog > /dev/null; then
        echo "Shutdown failed"
        exit 1
    fi
}

run_pgdog
wait_for_pgdog

active_venv

pushd ${SCRIPT_DIR}
python shutdown.py pgdog
popd

wait_for_shutdown

run_pgdog
wait_for_pgdog

pushd ${SCRIPT_DIR}
python shutdown.py pgdog_sharded
popd

wait_for_shutdown

stop_pgdog
