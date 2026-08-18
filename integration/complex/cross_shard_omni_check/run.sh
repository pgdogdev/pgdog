#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../../common.sh

export PGPASSWORD=pgdog

active_venv

run_pgdog "${SCRIPT_DIR}"
wait_for_pgdog

pushd ${SCRIPT_DIR}
python run.py
popd

stop_pgdog
