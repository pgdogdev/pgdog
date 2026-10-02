#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/shared.sh"

initialize_resharding
RESHARD_TASK_ID=$(psql admin -tAc 'RESHARD source destination pgdog')
prepare_resharding_verification
wait_for_replication_catchup
verify_resharding_data
wait_admin_child_inner_status "${RESHARD_TASK_ID}" replication 'reverse replicating'
write_replication_sentinel -1
wait_for_replication_catchup
