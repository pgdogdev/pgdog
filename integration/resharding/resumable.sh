#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/shared.sh"

slot_count() {
    pg_sql "$1" "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '$2%'"
}

origin_count() {
    pg_sql "$1" "SELECT count(*) FROM pg_replication_origin WHERE roname LIKE '__pgdog_origin_$2%'"
}

resharding_rows() {
    if [ "$(pg_sql 15434 "SELECT to_regclass('pgdog.resharding') IS NOT NULL")" = t ]; then
        pg_sql 15434 "SELECT count(*) FROM pgdog.resharding WHERE slot_name = '$1'"
    else
        echo 0
    fi
}

resharding_record() {
    pg_sql 15434 "SELECT command->>'command', publication, stage, slot_existed::text, config->'destination'->>'shards' FROM pgdog.resharding WHERE slot_name = '$1'"
}

lock_holders() {
    pg_sql 15434 "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid WHERE l.locktype = 'advisory' AND l.granted AND a.application_name LIKE 'pgdog_resharding:%'"
}

initialize_resharding

# A resumable run records itself and holds the lock while it runs.
COPY_RESULT=$(psql admin -tAc 'COPY_DATA source destination pgdog WITH (cleanup_on_cancel false)')
COPY_TASK_ID=${COPY_RESULT%%|*}
SLOT=${COPY_RESULT#*|}
test -n "${SLOT}"
test "$(admin_task_type "${COPY_TASK_ID}")" = 'reshard source -> destination (resumable)'
wait_admin_task_inner_status "${COPY_TASK_ID}" 'replicating'
test "$(resharding_record "${SLOT}")" = 'reshard|pgdog|replication|false|2'
test "$(lock_holders)" = 1

# STOP_TASK pauses it: the record, slots and origins stay, the lock is released.
psql admin -c "STOP_TASK ${COPY_TASK_ID}"
wait_admin_task_status "${COPY_TASK_ID}" 'cancelled'
test "$(resharding_record "${SLOT}")" = 'reshard|pgdog|replication|false|2'
test "$(slot_count 15432 "${SLOT}_0")" = 1
test "$(slot_count 15433 "${SLOT}_1")" = 1
test "$(origin_count 15434 "${SLOT}")" = 2
test "$(origin_count 15435 "${SLOT}")" = 2
# The lock is released when Postgres sees the closed connection, a moment later.
wait_for_value 0 lock_holders

# A plain run writes no record and drops its slots when stopped.
PLAIN_TASK_ID=$(psql admin -tAc 'REPLICATE source destination pgdog plain_slot WITH (cleanup_on_cancel true)')
# The slots are created after the reply, and dropped in the background.
wait_for_value 1 slot_count 15432 plain_slot
wait_for_value 1 slot_count 15433 plain_slot
psql admin -c "STOP_TASK ${PLAIN_TASK_ID}"
wait_admin_task_status "${PLAIN_TASK_ID}" 'cancelled'
test "$(resharding_rows plain_slot)" = 0
wait_for_value 0 slot_count 15432 plain_slot
wait_for_value 0 slot_count 15433 plain_slot
