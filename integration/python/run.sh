#!/bin/bash
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

bash ${SCRIPT_DIR}/../ci/apt.sh python3-virtualenv

run_pgdog
wait_for_pgdog

source ${SCRIPT_DIR}/dev.sh

stop_pgdog
run_pgdog integration/python/gate
wait_for_pgdog

pushd ${SCRIPT_DIR}
source venv/bin/activate
PGDOG_LEAK_DATABASES=pgdog_leak_auto pytest -x test_session_params_leak.py
deactivate
popd

stop_pgdog
