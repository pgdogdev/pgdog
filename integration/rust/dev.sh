#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
set -e

pushd ${SCRIPT_DIR}
cargo nextest run --no-fail-fast --test-threads=1
popd
