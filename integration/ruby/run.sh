#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

# Native gem extensions (psych, pg) need yaml + libpq headers.
bash ${SCRIPT_DIR}/../ci/apt.sh ruby-dev libyaml-dev libpq-dev build-essential
command -v bundle >/dev/null || sudo gem install bundler --no-document

run_pgdog
wait_for_pgdog

bash ${SCRIPT_DIR}/dev.sh

stop_pgdog
