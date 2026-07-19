#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

# Ruby specs; psych + pg need yaml + libpq headers to build.
bash ${SCRIPT_DIR}/../ci/apt.sh ruby-dev libyaml-dev libpq-dev build-essential
command -v bundle >/dev/null || sudo gem install bundler --no-document

run_pgdog
wait_for_pgdog

pushd ${SCRIPT_DIR}

bash dev.sh

popd

stop_pgdog
