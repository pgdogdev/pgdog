#!/bin/bash
set -e
THIS_SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${THIS_SCRIPT_DIR}/setup.sh
source ${THIS_SCRIPT_DIR}/toxi/setup.sh
pushd ${THIS_SCRIPT_DIR}/../
export NODE_ID=pgdog-dev-1
CMD="cargo run -- --config ${THIS_SCRIPT_DIR}/pgdog.toml --users ${THIS_SCRIPT_DIR}/users.toml"

if [[ -z "$1" ]]; then
    cargo watch --shell "${CMD}"
else
    ${CMD}
fi
popd
