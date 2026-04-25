#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pushd ${SCRIPT_DIR}
bash shutdown.sh
bash passthrough_auth/run.sh
bash cancel_query/run.sh
bash session_listen/run.sh
bash protocol_version/run.sh
popd
