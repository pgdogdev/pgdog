#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

bash ${SCRIPT_DIR}/listen/run.sh
bash ${SCRIPT_DIR}/shutdown/run.sh
