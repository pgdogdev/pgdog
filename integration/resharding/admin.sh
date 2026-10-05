#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/shared.sh"

initialize_resharding
COPY_RESULT=$(psql admin -tAc 'COPY_DATA source destination pgdog')
COPY_TASK_ID=${COPY_RESULT%%|*}
COPY_SLOT=${COPY_RESULT#*|}
test -n "${COPY_SLOT}"
wait_admin_task_inner_status "${COPY_TASK_ID}" 'replicating'
prepare_resharding_verification
wait_for_replication_catchup
verify_resharding_data
