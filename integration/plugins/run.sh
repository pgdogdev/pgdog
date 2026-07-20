#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

# dev.sh runs rspec via bundler; native gem extensions need yaml + libpq headers.
bash ${SCRIPT_DIR}/../ci/apt.sh ruby-dev libyaml-dev libpq-dev build-essential
command -v bundle >/dev/null || sudo gem install bundler --no-document

export CARGO_TARGET_DIR=${SCRIPT_DIR}/target

function build_plugin() {
    if [ -n "${PGDOG_PLUGIN_FEATURES:-}" ]; then
        cargo build --release --no-default-features --features "${PGDOG_PLUGIN_FEATURES}"
    else
        cargo build --release
    fi
}

pushd ${SCRIPT_DIR}/test-plugins/test-plugin-compatible
build_plugin
popd

pushd ${SCRIPT_DIR}/test-plugins/test-plugin-auth
build_plugin
popd

pushd ${SCRIPT_DIR}/test-plugins/test-plugin-outdated
cargo build --release
popd

unset CARGO_TARGET_DIR

pushd ${SCRIPT_DIR}/../../plugins/pgdog-example-plugin
build_plugin
popd

export LD_LIBRARY_PATH=${SCRIPT_DIR}/target/release:${SCRIPT_DIR}/../../target/release
export DYLD_LIBRARY_PATH=${LD_LIBRARY_PATH}

run_pgdog $SCRIPT_DIR
wait_for_pgdog

bash ${SCRIPT_DIR}/dev.sh

stop_pgdog

# Phase 2: authentication plugin (auth_type = "plugin").
# Postgres-side prerequisites (impersonated role, target table) go in first,
# applied directly against PostgreSQL rather than through PgDog.
PGPASSWORD=pgdog psql -h 127.0.0.1 -p 5432 -U pgdog -d pgdog -v ON_ERROR_STOP=1 \
    -f ${SCRIPT_DIR}/auth/setup.sql

run_pgdog ${SCRIPT_DIR}/auth
wait_for_pgdog
pushd ${SCRIPT_DIR}
bundle exec rspec auth/auth_spec.rb
popd
stop_pgdog
