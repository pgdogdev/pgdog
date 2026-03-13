#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pushd ${SCRIPT_DIR}/ruby
export GEM_HOME=~/.gem
mkdir -p ${GEM_HOME}
bundle install
bundle exec rspec *_spec.rb
popd

pushd ${SCRIPT_DIR}/php
bash run.sh
popd
