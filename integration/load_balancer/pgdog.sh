#!/bin/bash
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd ${SCRIPT_DIR}/../../
cargo run --release -- --config ${SCRIPT_DIR}/pgdog.toml --users ${SCRIPT_DIR}/users.toml
