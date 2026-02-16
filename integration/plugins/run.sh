#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

export CARGO_TARGET_DIR=${SCRIPT_DIR}/target

pushd ${SCRIPT_DIR}/test-plugins/test-plugin-compatible
cargo build --release
popd

pushd ${SCRIPT_DIR}/test-plugins/test-plugin-outdated
cargo build --release
popd

pushd ${SCRIPT_DIR}/test-plugins/test-plugin-main
cargo build --release
popd

unset CARGO_TARGET_DIR

pushd ${SCRIPT_DIR}/../../plugins/pgdog-example-plugin
cargo build --release
popd

export LD_LIBRARY_PATH=${SCRIPT_DIR}/target/release:${SCRIPT_DIR}/../../target/release
export DYLD_LIBRARY_PATH=${LD_LIBRARY_PATH}

run_pgdog $SCRIPT_DIR
wait_for_pgdog

bash ${SCRIPT_DIR}/dev.sh

stop_pgdog
