#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEFAULT_BIN="${SCRIPT_DIR}/../../target/debug/pgdog"
PGDOG_BIN=${PGDOG_BIN:-$DEFAULT_BIN}
PGDOG_PID=""
PGBENCH_PID=""
MIGRATION_PID=""

# Run in our own process group so we can kill every child on exit.
set -m
cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM
    if [ "${exit_code}" -ne 0 ]; then
        echo ""
        echo "=== deadlock diagnostics ==="
        for port in 15434 15435; do
            echo "--- dest :${port} pg_stat_activity ---"
            PGPASSWORD=pgdog psql -h 127.0.0.1 -p "${port}" -U pgdog -d postgres -c \
                "SELECT pid, wait_event_type, wait_event, state, left(query, 100) AS query
                   FROM pg_stat_activity
                  WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()
                  ORDER BY pid;" || true
            echo "--- dest :${port} pg_locks ---"
            PGPASSWORD=pgdog psql -h 127.0.0.1 -p "${port}" -U pgdog -d postgres -c \
                "SELECT locktype, relation::regclass, mode, granted, pid
                   FROM pg_locks
                  WHERE relation IS NOT NULL
                  ORDER BY pid, granted DESC;" || true
        done
        echo "==========================="
        echo ""
    fi

    kill -TERM "${MIGRATION_PID:-}" 2>/dev/null || true
    kill -TERM "${PGBENCH_PID:-}" 2>/dev/null || true
    kill -TERM "${PGDOG_PID:-}" 2>/dev/null || true
    (cd "${SCRIPT_DIR}" && docker compose down) || true

    # Signal every process in this script's process group except ourselves.
    pkill -TERM -P $$ 2> /dev/null || true
    # Give children a moment to exit cleanly, then force-kill anything left.
    sleep 1
    pkill -KILL -P $$ 2> /dev/null || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

admin_task_row() {
    local target="$1"
    psql admin -tAc 'SHOW TASKS' | while IFS='|' read -r id parent kind status inner rest; do
        if [ "${id}" = "${target}" ]; then
            printf '%s|%s' "${status}" "${inner}"
            break
        fi
    done
}

wait_admin_task_finished() {
    local task_id="$1"
    local row status
    for _ in $(seq 1 600); do
        row=$(admin_task_row "${task_id}")
        status=${row%%|*}
        case "${status}" in
            finished) return 0 ;;
            failed:*|panicked:*|cancelled)
                echo "task ${task_id} ended with ${status}"
                psql admin -c 'SHOW TASKS'
                return 1
                ;;
        esac
        sleep 1
    done
    echo "task ${task_id} did not finish within 600s"
    psql admin -c 'SHOW TASKS'
    return 1
}

wait_admin_task_inner_status() {
    wait_admin_inner_status "task $1" "$2" admin_task_row "$1"
}

admin_child_row() {
    local parent_id="$1"
    local kind="$2"
    psql admin -tAc 'SHOW TASKS' | while IFS='|' read -r id parent type status inner rest; do
        if [ "${parent}" = "${parent_id}" ] && [ "${type%% *}" = "${kind}" ]; then
            printf '%s|%s' "${status}" "${inner}"
            break
        fi
    done
}

wait_admin_child_inner_status() {
    wait_admin_inner_status "$2 child of task $1" "$3" admin_child_row "$1" "$2"
}

wait_admin_inner_status() {
    local label="$1"
    local expected="$2"
    shift 2
    local row status inner
    for _ in $(seq 1 120); do
        row=$("$@")
        status=${row%%|*}
        inner=${row#*|}
        if [ "${inner}" = "${expected}" ]; then
            return 0
        fi
        case "${status}" in
            finished|failed:*|panicked:*|cancelled)
                echo "${label} reached ${status} while waiting for ${expected}"
                psql admin -c 'SHOW TASKS'
                return 1
                ;;
        esac
        sleep 1
    done
    echo "${label} did not reach ${expected} within 120s"
    psql admin -c 'SHOW TASKS'
    return 1
}

run_finished_admin_task() {
    local task_id
    task_id=$(psql admin -tAc "$1")
    wait_admin_task_finished "${task_id}"
}

initialize_resharding() {
    pushd "${SCRIPT_DIR}"
    docker compose down && docker compose up -d

    # Give it a second to boot up.
    # It restarts the DB during initialization.
    sleep 2

    for port in 15432 15433 15434 15435; do
        echo "Waiting for database on port ${port}..."
        until PGPASSWORD=pgdog pg_isready -h 127.0.0.1 -p "${port}" -U pgdog -d postgres; do
            sleep 1
        done
    done

    "${PGDOG_BIN}" &
    PGDOG_PID="$!"

    export PGPASSWORD=pgdog
    export PGHOST=127.0.0.1
    export PGPORT=6432
    export PGUSER=pgdog

    until psql source -c 'SELECT 1' 2> /dev/null; do
        sleep 1
    done

    pgbench -f pgbench.sql -P 1 source -c 5 -t 1000000 &
    PGBENCH_PID="$!"

    sleep 10
}

replace_copy_with_replicate() {
    local table="$1"
    local column="$2"

    # Capture `UPDATE N` from psql so we can log how many -copy rows were cleaned up.
    local tag
    tag=$(psql source -c "UPDATE ${table} SET ${column} = regexp_replace(${column}, '-copy\$', '-replicate') WHERE ${column} LIKE '%-copy';" | grep -E '^UPDATE')
    local updated="${tag#UPDATE }"
    echo "${table}.${column}: replace_copy updated ${updated} rows on source"
}

prepare_resharding_verification() {
    sleep 10

    kill -TERM "${PGBENCH_PID}" 2>/dev/null || true
    wait "${PGBENCH_PID}" 2>/dev/null || true
    PGBENCH_PID=""

    replace_copy_with_replicate tenants name
    replace_copy_with_replicate accounts full_name
    replace_copy_with_replicate projects name
    replace_copy_with_replicate tasks title
    replace_copy_with_replicate task_comments body
    replace_copy_with_replicate settings name
    replace_copy_with_replicate sharded_to_omni name
    replace_copy_with_replicate omni_to_sharded name

    # REPLICATION SENTINEL — must be the last DML issued against the source.
    # pgbench uses random(1, 1_000_000_000), so id=0 is reserved for this purpose.
    # WAL is ordered on each shard: all four physical shards must receive the sentinel
    # before count comparisons, whether replication runs forward or reverse.
    # settings is an omni table: pgdog broadcasts the insert to all source shards.
    write_replication_sentinel 0
}

write_replication_sentinel() {
    SENTINEL_ID="$1"
    psql source -c "INSERT INTO settings (id, name, value) VALUES (${SENTINEL_ID}, 'sentinel_done', 'sentinel_done')"
}

wait_for_replication_catchup() {
    local deadline port count
    local -a missing
    echo "Waiting for the replication sentinel on all four shards (settings.id=${SENTINEL_ID}, timeout 120s)..."
    deadline=$((SECONDS + 120))
    while true; do
        missing=()
        for port in 15432 15433 15434 15435; do
            count=$(PGPASSWORD=pgdog psql -h 127.0.0.1 -p "${port}" -U pgdog -d postgres -tAc \
                "SELECT COUNT(*) FROM settings WHERE id = ${SENTINEL_ID} AND name = 'sentinel_done'" \
                2>/dev/null) || count=0
            if [ "${count}" != 1 ]; then
                missing+=(":${port}=${count}")
            fi
        done
        if [ "${#missing[@]}" -eq 0 ]; then
            break
        fi
        if [ -n "${MIGRATION_PID}" ] && ! kill -0 "${MIGRATION_PID}" 2>/dev/null; then
            echo "ERROR: migration process exited before replication caught up"
            return 1
        fi
        if ! kill -0 "${PGDOG_PID}" 2>/dev/null; then
            echo "ERROR: PgDog exited before replication caught up"
            return 1
        fi
        if [ "${SECONDS}" -ge "${deadline}" ]; then
            echo "ERROR: replication sentinel did not reach all four shards within 120s"
            printf '  %s\n' "${missing[@]}"
            for port in 15432 15433 15434 15435; do
                PGPASSWORD=pgdog psql -h 127.0.0.1 -p "${port}" -U pgdog -d postgres -c \
                    "SELECT slot_name, active, confirmed_flush_lsn, pg_current_wal_lsn() - confirmed_flush_lsn AS lag_bytes FROM pg_replication_slots" || true
            done
            return 1
        fi
        sleep 1
    done
    echo "Replication caught up on all four shards"
}

wait_for_no_copy_rows() {
    local table="$1"
    local column="$2"
    local deadline=$((SECONDS + 120))
    local src_copy

    while true; do
        src_copy=$(psql -d source -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM ${table} WHERE ${column} LIKE '%-copy'") || return 1
        if [ "${src_copy}" -eq 0 ]; then
            echo "${table}.${column}: source clean"
            return
        fi
        if ! kill -0 "${PGDOG_PID}" 2>/dev/null; then
            echo "ERROR: PgDog exited while checking ${table}.${column}"
            return 1
        fi
        if [ "${SECONDS}" -ge "${deadline}" ]; then
            echo "FAIL ${table}.${column}: source still has ${src_copy} -copy rows"
            return 1
        fi
        sleep 1
    done
}

# pg_count PORT TABLE — row count via a direct postgres connection (bypasses pgdog).
pg_count() { PGPASSWORD=pgdog psql -h 127.0.0.1 -p "$1" -U pgdog -d postgres -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM $2"; }

check_row_count_matches() {
    local table="$1"
    local source0_count source1_count destination0_count destination1_count
    local source_count destination_count

    source0_count=$(pg_count 15432 "${table}") || return 1
    source1_count=$(pg_count 15433 "${table}") || return 1
    destination0_count=$(pg_count 15434 "${table}") || return 1
    destination1_count=$(pg_count 15435 "${table}") || return 1
    source_count=$((source0_count + source1_count))
    destination_count=$((destination0_count + destination1_count))

    if [ "${source_count}" -ne "${destination_count}" ]; then
        echo "MISMATCH ${table}: source=${source_count} (${source0_count}/${source1_count}) destination=${destination_count} (${destination0_count}/${destination1_count})"
        return 1
    fi

    echo "OK ${table}: ${source_count} rows"
}

# check_omni_each_shard TABLE
# For omni (non-sharded) tables: queries each destination shard directly and asserts
# it holds the full source row count. A query through pgdog hits one shard and cannot
# detect a shard that is missing rows.
check_omni_each_shard() {
    local table="$1"
    local source_count dest0_count dest1_count

    source_count=$(pg_count 15432 "${table}") || return 1
    dest0_count=$(pg_count 15434 "${table}") || return 1
    dest1_count=$(pg_count 15435 "${table}") || return 1

    if [ "${source_count}" -ne "${dest0_count}" ] || [ "${source_count}" -ne "${dest1_count}" ]; then
        echo "MISMATCH omni ${table}: source=${source_count} dest-0(15434)=${dest0_count} dest-1(15435)=${dest1_count} (expected ${source_count} on each)"
        return 1
    fi

    echo "OK omni ${table}: ${source_count} rows on each shard"
}

check_sharded_source_to_omni_destination() {
    local table="$1"
    local source0_count source1_count source_count dest0_count dest1_count

    source0_count=$(pg_count 15432 "${table}") || return 1
    source1_count=$(pg_count 15433 "${table}") || return 1
    source_count=$((source0_count + source1_count))
    dest0_count=$(pg_count 15434 "${table}") || return 1
    dest1_count=$(pg_count 15435 "${table}") || return 1

    if [ "${source_count}" -ne "${dest0_count}" ] || [ "${source_count}" -ne "${dest1_count}" ]; then
        echo "MISMATCH sharded->omni ${table}: source=${source_count} dest-0(15434)=${dest0_count} dest-1(15435)=${dest1_count} (expected ${source_count} on each)"
        return 1
    fi

    echo "OK sharded->omni ${table}: ${source_count} rows on each destination shard"
}

check_omni_source_to_sharded_destination() {
    local table="$1"
    local source0_count source1_count dest0_count dest1_count dest_total

    source0_count=$(pg_count 15432 "${table}") || return 1
    source1_count=$(pg_count 15433 "${table}") || return 1
    if [ "${source0_count}" -ne "${source1_count}" ]; then
        echo "MISMATCH omni->sharded ${table}: source shards disagree source-0(15432)=${source0_count} source-1(15433)=${source1_count}"
        return 1
    fi

    dest0_count=$(pg_count 15434 "${table}") || return 1
    dest1_count=$(pg_count 15435 "${table}") || return 1
    dest_total=$((dest0_count + dest1_count))

    if [ "${source0_count}" -ne "${dest_total}" ]; then
        echo "MISMATCH omni->sharded ${table}: source=${source0_count} dest total=${dest_total} (dest-0=${dest0_count} dest-1=${dest1_count})"
        return 1
    fi

    echo "OK omni->sharded ${table}: ${source0_count} rows split ${dest0_count}/${dest1_count}"
}

# check_constraints_validated — every destination shard must have each schema-synced
# constraint present and validated (pg_constraint.convalidated), not left NOT VALID.
# Validation can run beside replication, so wait up to 120s for it to finish.
check_constraints_validated() {
    local source_total port not_valid dest_total deadline

    source_total=$(PGPASSWORD=pgdog psql -h 127.0.0.1 -p 15432 -U pgdog -d postgres -v ON_ERROR_STOP=1 -tAc \
        "SELECT COUNT(*) FROM pg_constraint WHERE contype IN ('f', 'c') AND connamespace = 'public'::regnamespace") || return 1

    for port in 15434 15435; do
        dest_total=$(PGPASSWORD=pgdog psql -h 127.0.0.1 -p "${port}" -U pgdog -d postgres -v ON_ERROR_STOP=1 -tAc \
            "SELECT COUNT(*) FROM pg_constraint WHERE contype IN ('f', 'c') AND connamespace = 'public'::regnamespace") || return 1
        if [ "${dest_total}" -ne "${source_total}" ]; then
            echo "MISMATCH constraints on :${port}: source=${source_total} destination=${dest_total}"
            return 1
        fi
    done

    deadline=$((SECONDS + 120))
    for port in 15434 15435; do
        while true; do
            not_valid=$(PGPASSWORD=pgdog psql -h 127.0.0.1 -p "${port}" -U pgdog -d postgres -v ON_ERROR_STOP=1 -tAc \
                "SELECT string_agg(conname, ', ' ORDER BY conname) FROM pg_constraint WHERE connamespace = 'public'::regnamespace AND NOT convalidated") || return 1
            if [ -z "${not_valid}" ]; then
                break
            fi
            if [ "${SECONDS}" -ge "${deadline}" ]; then
                echo "FAIL constraints on :${port} are not validated within 120s: ${not_valid}"
                psql admin -c 'SHOW TASKS' || true
                return 1
            fi
            sleep 1
        done
    done

    echo "OK constraints: ${source_total} validated on each destination shard"
}

verify_resharding_data() {
    local failed=0
    local check

    for check in \
        "wait_for_no_copy_rows tenants name" \
        "wait_for_no_copy_rows accounts full_name" \
        "wait_for_no_copy_rows projects name" \
        "wait_for_no_copy_rows tasks title" \
        "wait_for_no_copy_rows task_comments body" \
        "wait_for_no_copy_rows settings name" \
        "wait_for_no_copy_rows sharded_to_omni name" \
        "wait_for_no_copy_rows omni_to_sharded name" \
        "check_row_count_matches tenants" \
        "check_row_count_matches accounts" \
        "check_row_count_matches projects" \
        "check_row_count_matches tasks" \
        "check_row_count_matches task_comments" \
        "check_omni_each_shard settings" \
        "check_sharded_source_to_omni_destination sharded_to_omni" \
        "check_omni_source_to_sharded_destination omni_to_sharded" \
        "check_constraints_validated"; do
        ${check} || failed=1
    done

    return "${failed}"
}
