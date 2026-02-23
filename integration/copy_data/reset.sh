#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd $SCRIPT_DIR
cp pgdog.bak.toml pgdog.toml
cp users.bak.toml users.toml
popd
