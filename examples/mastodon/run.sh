#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd ${SCRIPT_DIR}/../../
cargo run -- \
    --config examples/mastodon/pgdog.toml \
    --users examples/mastodon/users.toml
popd
