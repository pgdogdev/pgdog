#!/bin/bash
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd "${SCRIPT_DIR}"

if [[ ! -x venv/bin/python ]]; then
    python3 -m venv venv
fi

venv/bin/python -m pip install -r requirements.txt

# Requires PgDog on 127.0.0.1:6432 and configured AWS credentials.
venv/bin/python -m pytest test_rds_iam.py -v --tb=short "$@"
