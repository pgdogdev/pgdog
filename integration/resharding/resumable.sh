#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/shared.sh"

source_slots() {
    local first second
    first=$(pg_sql 15432 "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '$1_0'") || return 1
    second=$(pg_sql 15433 "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '$1_1'") || return 1
    echo $((first + second))
}

# command|state|replication_slot of the source -> destination resharding.
resharding() {
    local rows
    rows=$(psql admin -tAc 'SHOW RESHARDING') || return 1
    grep '^source|destination|' <<< "${rows}" | cut -d'|' -f3,6,9 || true
}

task_status() {
    local row
    row=$(admin_task_row "$1") || return 1
    echo "${row%%|*}" | cut -d: -f1
}

NO_RESHARDING='no unfinished resharding source -> destination (pgdog)'

# An abort must fail at once with the given error.
abort_fails_with() {
    local error
    if error=$(psql admin -tAc "$1" 2>&1); then
        echo "$1 must fail, returned: ${error}" >&2
        return 1
    fi
    if [[ "${error}" != *"$2"* ]]; then
        echo "$1 failed with: ${error}, expected: $2" >&2
        return 1
    fi
}

initialize_resharding

# Abort before any run is refused.
abort_fails_with 'COPY_DATA source destination pgdog WITH (abort)' "${NO_RESHARDING}"

# A resumable run shows as running while it runs.
COPY_RESULT=$(psql admin -tAc 'COPY_DATA source destination pgdog WITH (cleanup_on_cancel false)')
COPY_TASK_ID=${COPY_RESULT%%|*}
SLOT=${COPY_RESULT#*|}
test -n "${SLOT}"
wait_admin_task_inner_status "${COPY_TASK_ID}" 'replicating'
test "$(resharding)" = "reshard|running|${SLOT}"

# STOP_TASK pauses it and keeps its slots.
psql admin -c "STOP_TASK ${COPY_TASK_ID}"
wait_admin_task_status "${COPY_TASK_ID}" 'cancelled'
wait_for_value "reshard|paused|${SLOT}" resharding
test "$(source_slots "${SLOT}")" = 2

# The pause survives a restart.
kill -TERM "${PGBENCH_PID}" 2>/dev/null || true
wait "${PGBENCH_PID}" || true
PGBENCH_PID=""
restart_pgdog
wait_for_value "reshard|paused|${SLOT}" resharding
test "$(source_slots "${SLOT}")" = 2

# A new run with the same key is refused while the paused run exists.
NEW_TASK_ID=$(psql admin -tAc 'COPY_DATA source destination pgdog' | cut -d'|' -f1)
wait_for_value failed task_status "${NEW_TASK_ID}"
test "$(resharding)" = "reshard|paused|${SLOT}"

# Abort with the wrong command is refused.
abort_fails_with 'REPLICATE source destination pgdog WITH (abort)' \
    'resharding command is "reshard", not "replicate"'

# Abort removes the run and its slots.
ABORT_TASK_ID=$(psql admin -tAc 'COPY_DATA source destination pgdog WITH (abort)')
wait_admin_task_finished "${ABORT_TASK_ID}"
RESHARDING=$(resharding)
test -z "${RESHARDING}"
test "$(source_slots "${SLOT}")" = 0

# A second abort is refused: the run is gone.
abort_fails_with 'COPY_DATA source destination pgdog WITH (abort)' "${NO_RESHARDING}"

# A plain run removes its slots and leaves no run on STOP_TASK.
PLAIN_TASK_ID=$(psql admin -tAc 'REPLICATE source destination pgdog plain_slot')
wait_for_value 'replicate|running|plain_slot' resharding
psql admin -c "STOP_TASK ${PLAIN_TASK_ID}"
wait_admin_task_status "${PLAIN_TASK_ID}" 'cancelled'
wait_for_value '' resharding
wait_for_value 0 source_slots plain_slot
