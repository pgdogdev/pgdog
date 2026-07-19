#!/bin/bash
#
# Shared helpers for ruby integration suites.
# Source this file; do not execute directly.
#
RUBY_COMMON_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${RUBY_COMMON_DIR}/../common.sh"

# Install system packages and the bundler gem. Call once per CI run.
function install_deps() {
    # Native gem extensions (psych, pg) need yaml + libpq headers.
    bash ${RUBY_COMMON_DIR}/../ci/apt.sh ruby-dev libyaml-dev libpq-dev build-essential
    command -v bundle >/dev/null || sudo gem install bundler --no-document
}

# Run bundle install and rspec in TARGET_DIR using the shared Gemfile.
# Defaults to RUBY_COMMON_DIR when called with no argument.
function dev_suite() {
    local target_dir="${1:-$RUBY_COMMON_DIR}"

    export BUNDLE_GEMFILE="${RUBY_COMMON_DIR}/Gemfile"
    export GEM_HOME=~/.gem
    mkdir -p ${GEM_HOME}

    pushd "${target_dir}"
    bundle install
    bundle exec rspec *_spec.rb
    popd
}

# Full CI cycle for a single suite: start pgdog, run tests, stop.
# CONFIG_DIR is optional — omit to use the default integration/ config.
# Call install_deps before the first run_suite in a process.
function run_suite() {
    local config_dir="${1:-}"

    if [ -n "$config_dir" ]; then
        run_pgdog "$config_dir"
    else
        run_pgdog
    fi
    wait_for_pgdog

    dev_suite "$config_dir"

    stop_pgdog
}
