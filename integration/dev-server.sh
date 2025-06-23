#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/setup.sh
source ${SCRIPT_DIR}/toxi/setup.sh
pushd ${SCRIPT_DIR}/../
cargo watch --shell "cargo run -- --config integration/pgdog.toml --users integration/users.toml"
popd
