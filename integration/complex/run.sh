#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# shutdown.sh and cancel_query use active_venv (python virtualenv).
bash ${SCRIPT_DIR}/../ci/apt.sh python3-virtualenv

pushd ${SCRIPT_DIR}
bash shutdown.sh
bash passthrough_auth/run.sh
bash cancel_query/run.sh
bash session_listen/run.sh
bash protocol_version/run.sh
popd
