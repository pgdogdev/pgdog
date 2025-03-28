#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

run_pgdog
wait_for_pgdog

pushd ${SCRIPT_DIR}

export GEM_HOME=~/.gem
mkdir -p ${GEM_HOME}
bundle install
bundle exec rspec pg_spec.rb

popd

stop_pgdog
