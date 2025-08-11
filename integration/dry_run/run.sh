#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

run_pgdog $SCRIPT_DIR
wait_for_pgdog

bash ${SCRIPT_DIR}/dev.sh

stop_pgdog
