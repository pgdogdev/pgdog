#!/bin/bash
# Run only the cache integration tests.
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pushd "${SCRIPT_DIR}"
cargo nextest run --nff -j 1 integration
popd
