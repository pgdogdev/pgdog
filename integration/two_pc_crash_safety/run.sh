#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

bash "${SCRIPT_DIR}/setup.sh"

pushd "${SCRIPT_DIR}" >/dev/null
bundle install
bundle exec rspec crash_recovery_spec.rb -fd
popd >/dev/null
