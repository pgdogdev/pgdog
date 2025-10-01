#!/bin/bash
set -e
THIS_SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${THIS_SCRIPT_DIR}/../toxi/setup.sh
pushd ${THIS_SCRIPT_DIR}
cargo run
popd
