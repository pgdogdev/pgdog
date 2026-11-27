#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/shared.sh"

SLOT=resharding_resume

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

resume_replication() {
    local task_id
    task_id=$(psql admin -tAc 'REPLICATE source destination pgdog WITH (resume)') || return 1
    wait_admin_task_inner_status "${task_id}" 'replicating' >&2 || return 1
    wait_for_value "replicate|running|${SLOT}" resharding >&2 || return 1
    echo "${task_id}"
}

initialize_resharding

# Abort before any run is refused.
abort_fails_with 'COPY_DATA source destination pgdog WITH (abort)' "${NO_RESHARDING}"

pgdog_cli schema-sync
pgdog_cli data-sync --sync-only --skip-schema-sync --replication-slot "${SLOT}"
pgdog_cli schema-sync --phase post

# A resumable run shows as running while it runs.
TASK_ID=$(psql admin -tAc "REPLICATE source destination pgdog ${SLOT} WITH (cleanup_on_cancel false)")
wait_admin_task_inner_status "${TASK_ID}" 'replicating'
wait_for_value "replicate|running|${SLOT}" resharding

# STOP_TASK pauses it and keeps its slots.
psql admin -c "STOP_TASK ${TASK_ID}"
wait_admin_task_status "${TASK_ID}" 'cancelled'
wait_for_value "replicate|paused|${SLOT}" resharding
test "$(source_slots "${SLOT}")" = 2

# The pause survives a restart.
stop_pgbench
restart_pgdog
wait_for_value "replicate|paused|${SLOT}" resharding
test "$(source_slots "${SLOT}")" = 2

# A new run with the same key is refused while the paused run exists.
NEW_TASK_ID=$(psql admin -tAc 'COPY_DATA source destination pgdog' | cut -d'|' -f1)
wait_for_value failed task_status "${NEW_TASK_ID}"
test "$(resharding)" = "replicate|paused|${SLOT}"

TASK_ID=$(resume_replication)
start_pgbench
sleep 5

kill_pgdog
stop_pgbench
start_pgdog
wait_for_pgdog_admin
wait_for_value "replicate|paused|${SLOT}" resharding
test "$(source_slots "${SLOT}")" = 2

TASK_ID=$(resume_replication)
start_pgbench
prepare_resharding_verification
wait_for_replication_catchup
pgdog_cli schema-sync --validation
verify_resharding_data
wait_admin_task_inner_status "${TASK_ID}" 'replicating'

# Abort with the wrong command is refused.
abort_fails_with 'COPY_DATA source destination pgdog WITH (abort)' \
    'resharding command is "replicate", not "reshard"'

psql admin -c "STOP_TASK ${TASK_ID}"
wait_admin_task_status "${TASK_ID}" 'cancelled'
wait_for_value "replicate|paused|${SLOT}" resharding

# Abort removes the run and its slots.
ABORT_TASK_ID=$(psql admin -tAc 'REPLICATE source destination pgdog WITH (abort)')
wait_admin_task_finished "${ABORT_TASK_ID}"
RESHARDING=$(resharding)
test -z "${RESHARDING}"
test "$(source_slots "${SLOT}")" = 0

# A second abort is refused: the run is gone.
abort_fails_with 'REPLICATE source destination pgdog WITH (abort)' "${NO_RESHARDING}"

# A plain run removes its slots and leaves no run on STOP_TASK.
PLAIN_TASK_ID=$(psql admin -tAc 'REPLICATE source destination pgdog plain_slot')
wait_for_value 'replicate|running|plain_slot' resharding
psql admin -c "STOP_TASK ${PLAIN_TASK_ID}"
wait_admin_task_status "${PLAIN_TASK_ID}" 'cancelled'
wait_for_value '' resharding
wait_for_value 0 source_slots plain_slot
