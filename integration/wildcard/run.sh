#!/bin/bash
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

run_pgdog ${SCRIPT_DIR}
wait_for_pgdog

pushd ${SCRIPT_DIR}

python3 test_wildcard.py

popd

stop_pgdog
