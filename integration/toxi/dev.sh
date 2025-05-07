#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export GEM_HOME=~/.gem

pushd ${SCRIPT_DIR}
bundle install
bundle exec rspec *_spec.rb -fd --fail-fast
popd
