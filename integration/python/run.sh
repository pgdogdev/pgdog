#!/bin/bash
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

bash ${SCRIPT_DIR}/../ci/apt.sh python3-virtualenv

run_pgdog
wait_for_pgdog

source ${SCRIPT_DIR}/dev.sh

stop_pgdog
