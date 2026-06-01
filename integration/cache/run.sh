#!/bin/bash
# Run cache integration tests with the dedicated cache pgdog config.
# PostgreSQL must be running on 127.0.0.1:5432 and Redis on 127.0.0.1:6379.
# Run integration/setup.sh first if you haven't already.
# Run integration/python/run.sh first to install python dependencies.
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}"/../common.sh

run_pgdog "integration/cache"
wait_for_pgdog

bash "${SCRIPT_DIR}"/dev.sh

stop_pgdog
