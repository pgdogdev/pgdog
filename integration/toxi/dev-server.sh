#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd ${SCRIPT_DIR}/../../
cargo watch --shell "cargo run -- --config integration/pgdog.toml --users integration/users.toml"
popd
