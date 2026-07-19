#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

# Runs both ruby and php; install all their build-time deps.
bash ${SCRIPT_DIR}/../ci/apt.sh \
    ruby-dev libyaml-dev libpq-dev build-essential \
    php-cli php-pgsql
command -v bundle >/dev/null || sudo gem install bundler --no-document

run_pgdog $SCRIPT_DIR
wait_for_pgdog

bash ${SCRIPT_DIR}/dev.sh


pushd ${SCRIPT_DIR}/php
bash run.sh
popd


stop_pgdog
