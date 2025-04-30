#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd ${SCRIPT_DIR}/../../../

if [[ "$1" == "sample" ]]; then
    samply record target/debug/pgdog --config ${SCRIPT_DIR}/pgdog.toml --users ${SCRIPT_DIR}/users.toml
elif [[ "$1" == "dev" ]]; then
    cargo run -- --config ${SCRIPT_DIR}/pgdog.toml --users ${SCRIPT_DIR}/users.toml
else
    cargo run --release -- --config ${SCRIPT_DIR}/pgdog.toml --users ${SCRIPT_DIR}/users.toml
fi
