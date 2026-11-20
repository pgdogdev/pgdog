#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/shared.sh"

resharding_records() {
    local exists
    exists=$(pg_sql 15434 "SELECT to_regclass('__pgdog.resharding') IS NOT NULL") || return 1
    if [ "${exists}" = t ]; then
        pg_sql 15434 'SELECT count(*) FROM __pgdog.resharding'
    else
        echo 0
    fi
}

slot_count() {
    pg_sql "$1" "SELECT count(*) FROM pg_replication_slots WHERE starts_with(slot_name::text, '$2')"
}

origin_count() {
    pg_sql "$1" "SELECT count(*) FROM pg_replication_origin WHERE starts_with(roname, '__pgdog_origin_$2')"
}

lock_holders() {
    pg_sql 15434 "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid WHERE l.locktype = 'advisory' AND l.granted AND starts_with(a.application_name, 'pgdog_resharding:')"
}

initialize_resharding
RESHARD_TASK_ID=$(psql admin -tAc 'RESHARD source destination pgdog WITH (cleanup_on_cancel false)')
# The record is written inside the task.
wait_for_value 1 resharding_records
SLOT=$(pg_sql 15434 'SELECT slot_name FROM __pgdog.resharding')
prepare_resharding_verification
wait_for_replication_catchup
verify_resharding_data
wait_admin_task_inner_status "${RESHARD_TASK_ID}" 'reverse replicating'

test "$(resharding_records)" = 0
wait_for_value 0 lock_holders
RESHARDING=$(psql admin -tAc 'SHOW RESHARDING')
test -z "${RESHARDING}"
test "$(slot_count 15432 "${SLOT}_0")" = 0
test "$(slot_count 15433 "${SLOT}_1")" = 0
test "$(slot_count 15434 "${SLOT}_0")" = 1
test "$(slot_count 15435 "${SLOT}_1")" = 1
wait_for_value 2 origin_count 15432 "${SLOT}"
wait_for_value 2 origin_count 15433 "${SLOT}"

if psql admin -tAc 'RESHARD source destination pgdog WITH (abort)'; then
    echo "abort after the cutover must fail" >&2
    exit 1
fi

psql admin -c "STOP_TASK ${RESHARD_TASK_ID}"
wait_admin_task_finished "${RESHARD_TASK_ID}"
wait_for_value 0 slot_count 15434 "${SLOT}_0"
wait_for_value 0 slot_count 15435 "${SLOT}_1"
wait_for_value 0 origin_count 15432 "${SLOT}"
wait_for_value 0 origin_count 15433 "${SLOT}"
test "$(resharding_records)" = 0
