#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

run_pgdog
wait_for_pgdog

pushd ${SCRIPT_DIR}

virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

pytest

popd

stop_pgdog
