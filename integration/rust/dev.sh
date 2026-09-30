#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
set -e

pushd ${SCRIPT_DIR}
cargo nextest run --profile integration ${NEXTEST_SHARD:+--partition count:${NEXTEST_SHARD} --no-fail-fast}
popd
