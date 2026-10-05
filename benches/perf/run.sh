#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../../integration/common.sh

run_pgdog benches/perf/$1
wait_for_pgdog

bash ${SCRIPT_DIR}/$1/run.sh

stop_pgdog
