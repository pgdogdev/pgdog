#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}"/../common.sh

active_venv

pushd "${SCRIPT_DIR}"
pytest -x test_cache.py
popd
