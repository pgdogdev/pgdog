#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd ${SCRIPT_DIR}
bundle config set --local path vendor/bundle
bundle install
bundle exec rspec *_spec.rb
popd
