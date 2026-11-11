#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/shared.sh"

initialize_resharding
# A resumable run deletes its record when the cutover starts.
RESHARD_TASK_ID=$(psql admin -tAc 'RESHARD source destination pgdog WITH (cleanup_on_cancel false)')
resharding_records() {
    if [ "$(pg_sql 15434 "SELECT to_regclass('pgdog.resharding') IS NOT NULL")" = t ]; then
        pg_sql 15434 'SELECT count(*) FROM pgdog.resharding'
    else
        echo 0
    fi
}
# The record is written inside the task, after the reply.
wait_for_value 1 resharding_records
prepare_resharding_verification
wait_for_replication_catchup
verify_resharding_data
wait_admin_task_inner_status "${RESHARD_TASK_ID}" 'reverse replicating'
test "$(pg_sql 15434 'SELECT count(*) FROM pgdog.resharding')" = 0
