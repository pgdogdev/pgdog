#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

bash "${SCRIPT_DIR}/../ci/apt.sh" python3-virtualenv

pushd "${SCRIPT_DIR}"

virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

python crash_recovery.py

deactivate
popd
