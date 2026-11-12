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

resharding_row() {
    psql admin -tAc 'SHOW RESHARDING' | grep '^source|destination|' || true
}

# SHOW RESHARDING without owner and last_task, which change between runs.
resharding_status() {
    resharding_row | cut -d'|' -f1-6,9-13
}

initialize_resharding

# A resumable run records itself and holds the lock while it runs.
COPY_RESULT=$(psql admin -tAc 'COPY_DATA source destination pgdog WITH (cleanup_on_cancel false)')
COPY_TASK_ID=${COPY_RESULT%%|*}
SLOT=${COPY_RESULT#*|}
test -n "${SLOT}"
test "$(admin_task_type "${COPY_TASK_ID}")" = 'reshard source -> destination'
wait_admin_task_inner_status "${COPY_TASK_ID}" 'replicating'
test "$(resharding_record "${SLOT}")" = 'reshard|pgdog|replication|false|2'
test "$(lock_holders)" = 1
test "$(resharding_row | cut -d'|' -f6,7)" = "running|$(psql admin -tAc 'SHOW INSTANCE_ID')"

# STOP_TASK pauses it: the record, slots and origins stay, the lock is released.
psql admin -c "STOP_TASK ${COPY_TASK_ID}"
wait_admin_task_status "${COPY_TASK_ID}" 'cancelled'
test "$(resharding_record "${SLOT}")" = 'reshard|pgdog|replication|false|2'
test "$(slot_count 15432 "${SLOT}_0")" = 1
test "$(slot_count 15433 "${SLOT}_1")" = 1
test "$(origin_count 15434 "${SLOT}")" = 2
test "$(origin_count 15435 "${SLOT}")" = 2
# The lock is released during the cleanup.
wait_for_value 0 lock_holders
PAUSED="source|destination|reshard|pgdog|replication|paused|${SLOT}|f|2/2|4/4|ok"
test "$(resharding_status)" = "${PAUSED}"

# The pause survives a restart: SHOW RESHARDING reads the database, not memory.
kill -TERM "${PGBENCH_PID}"
wait "${PGBENCH_PID}" || true
restart_pgdog
test "$(resharding_status)" = "${PAUSED}"

# Abort checks the stored command at once.
if MISMATCH=$(psql admin -tAc 'REPLICATE source destination pgdog WITH (abort)' 2>&1); then
    exit 1
fi
[[ "${MISMATCH}" == *'resharding command is "reshard", not "replicate"'* ]]

# Abort drops the slots the run created, all its origins and the record.
ABORT_TASK_ID=$(psql admin -tAc 'COPY_DATA source destination pgdog WITH (abort)')
test "$(admin_task_type "${ABORT_TASK_ID}")" = 'abort reshard source -> destination'
wait_admin_task_finished "${ABORT_TASK_ID}"
test "$(resharding_rows "${SLOT}")" = 0
test "$(slot_count 15432 "${SLOT}_0")" = 0
test "$(slot_count 15433 "${SLOT}_1")" = 0
test "$(origin_count 15434 "${SLOT}")" = 0
test "$(origin_count 15435 "${SLOT}")" = 0
test -z "$(resharding_row)"

# A slot that existed before the run stays after abort.
pg_sql 15432 "SELECT pg_create_logical_replication_slot('resumable_keep_0', 'pgoutput')"
pg_sql 15433 "SELECT pg_create_logical_replication_slot('resumable_keep_1', 'pgoutput')"
KEEP_TASK_ID=$(psql admin -tAc 'REPLICATE source destination pgdog resumable_keep WITH (cleanup_on_cancel false)')
wait_for_value 2 origin_count 15434 resumable_keep
psql admin -c "STOP_TASK ${KEEP_TASK_ID}"
wait_admin_task_status "${KEEP_TASK_ID}" 'cancelled'
test "$(resharding_record resumable_keep)" = 'replicate|pgdog|replication|true|2'
wait_for_value 0 lock_holders
KEEP_ABORT_ID=$(psql admin -tAc 'REPLICATE source destination pgdog WITH (abort)')
wait_admin_task_finished "${KEEP_ABORT_ID}"
test "$(resharding_rows resumable_keep)" = 0
test "$(slot_count 15432 resumable_keep_0)" = 1
test "$(slot_count 15433 resumable_keep_1)" = 1
test "$(origin_count 15434 resumable_keep)" = 0
test "$(origin_count 15435 resumable_keep)" = 0
pg_sql 15432 "SELECT pg_drop_replication_slot('resumable_keep_0')"
pg_sql 15433 "SELECT pg_drop_replication_slot('resumable_keep_1')"

# A plain run also records itself and holds the lock while it runs.
PLAIN_TASK_ID=$(psql admin -tAc 'REPLICATE source destination pgdog plain_slot WITH (cleanup_on_cancel true)')
# The slots are created after the reply, and dropped in the background.
wait_for_value 1 slot_count 15432 plain_slot
wait_for_value 1 slot_count 15433 plain_slot
test "$(resharding_record plain_slot)" = 'replicate|pgdog|replication|false|2'
test "$(lock_holders)" = 1

# STOP_TASK drops its slots and origins, deletes the record and releases the lock.
psql admin -c "STOP_TASK ${PLAIN_TASK_ID}"
wait_admin_task_status "${PLAIN_TASK_ID}" 'cancelled'
test "$(resharding_rows plain_slot)" = 0
wait_for_value 0 lock_holders
wait_for_value 0 slot_count 15432 plain_slot
wait_for_value 0 slot_count 15433 plain_slot
