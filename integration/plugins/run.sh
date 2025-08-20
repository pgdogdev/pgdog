#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

pushd ${SCRIPT_DIR}/../../plugins/pgdog-example-plugin
cargo build --release
popd

export LD_LIBRARY_PATH=${SCRIPT_DIR}/../../target/release
export DYLD_LIBRARY_PATH=${LD_LIBRARY_PATH}

run_pgdog $SCRIPT_DIR
wait_for_pgdog

bash ${SCRIPT_DIR}/dev.sh

stop_pgdog
