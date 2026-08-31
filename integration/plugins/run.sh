#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
source "${SCRIPT_DIR}/../common.sh"

build_plugin() {
  if [[ -n "${PGDOG_PLUGIN_FEATURES:-}" ]]; then
    cargo build --release --no-default-features --features "${PGDOG_PLUGIN_FEATURES}"
  else
    cargo build --release
  fi
}

main() {
  # dev.sh runs rspec via bundler; native gem extensions need yaml + libpq headers.
  bash "${SCRIPT_DIR}/../ci/apt.sh" ruby-dev libyaml-dev libpq-dev build-essential
  command -v bundle >/dev/null || sudo gem install bundler --no-document

  export CARGO_TARGET_DIR="${SCRIPT_DIR}/target"

  pushd "${SCRIPT_DIR}/test-plugins/test-plugin-compatible" >/dev/null
  build_plugin
  popd >/dev/null

  pushd "${SCRIPT_DIR}/test-plugins/test-plugin-auth" >/dev/null
  build_plugin
  popd >/dev/null

  pushd "${SCRIPT_DIR}/test-plugins/test-plugin-outdated" >/dev/null
  cargo build --release
  popd >/dev/null

  unset CARGO_TARGET_DIR

  pushd "${SCRIPT_DIR}/../../plugins/pgdog-example-plugin" >/dev/null
  build_plugin
  popd >/dev/null

  pushd "${SCRIPT_DIR}/../../plugins/pgdog-google-auth" >/dev/null
  cargo build --release
  popd >/dev/null

  export LD_LIBRARY_PATH="${SCRIPT_DIR}/target/release:${SCRIPT_DIR}/../../target/release"
  export DYLD_LIBRARY_PATH="${LD_LIBRARY_PATH}"

  run_pgdog "${SCRIPT_DIR}"
  wait_for_pgdog
  bash "${SCRIPT_DIR}/dev.sh"
  stop_pgdog

  # Phase 2: generic authentication plugin.
  PGPASSWORD=pgdog psql -h 127.0.0.1 -p 5432 -U pgdog -d pgdog -v ON_ERROR_STOP=1 \
    -f "${SCRIPT_DIR}/auth/setup.sql"

  run_pgdog "${SCRIPT_DIR}/auth"
  wait_for_pgdog
  pushd "${SCRIPT_DIR}" >/dev/null
  bundle exec rspec auth/auth_spec.rb
  popd >/dev/null
  stop_pgdog

  # Phase 3: Google access-token plugin with a local tokeninfo mock.
  PGPASSWORD=pgdog psql -h 127.0.0.1 -p 5432 -U pgdog -d pgdog -v ON_ERROR_STOP=1 \
    -f "${SCRIPT_DIR}/google/setup.sql"

  run_pgdog "${SCRIPT_DIR}/google"
  wait_for_pgdog
  pushd "${SCRIPT_DIR}" >/dev/null
  bundle exec rspec google/google_auth_spec.rb
  popd >/dev/null
  stop_pgdog
}

main "$@"
