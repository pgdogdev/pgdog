#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEFAULT_BIN="${SCRIPT_DIR}/../../target/debug/pgdog"
PGDOG_BIN=${PGDOG_BIN:-$DEFAULT_BIN}

# Run in our own process group so we can kill every child on exit.
set -m
cleanup() {
    trap - EXIT INT TERM
    # Signal every process in this script's process group except ourselves.
    pkill -TERM -P $$ 2> /dev/null || true
    # Give children a moment to exit cleanly, then force-kill anything left.
    sleep 1
    pkill -KILL -P $$ 2> /dev/null || true
}
trap cleanup EXIT INT TERM

pushd ${SCRIPT_DIR}
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

${PGDOG_BIN} &
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

psql admin -c 'COPY_DATA source destination pgdog'

sleep 10

kill -TERM ${PGBENCH_PID}

replace_copy_with_replicate() {
    local table="$1"
    local column="$2"

    # Capture `UPDATE N` from psql so we know how many rows the replication stream
    # must propagate before wait_for_no_copy_rows can succeed on this table.
    local tag
    tag=$(psql source -c "UPDATE ${table} SET ${column} = regexp_replace(${column}, '-copy\$', '-replicate') WHERE ${column} LIKE '%-copy';" | grep -E '^UPDATE')
    local updated="${tag#UPDATE }"
    echo "${table}.${column}: replace_copy updated ${updated} rows on source"
}

replace_copy_with_replicate tenants name
replace_copy_with_replicate accounts full_name
replace_copy_with_replicate projects name
replace_copy_with_replicate tasks title
replace_copy_with_replicate task_comments body
replace_copy_with_replicate settings name

wait_for_no_copy_rows() {
    local table="$1"
    local column="$2"

    # If source still has -copy rows, destination can never reach 0.
    local src_copy
    src_copy=$(psql -d source -tAc "SELECT COUNT(*) FROM ${table} WHERE ${column} LIKE '%-copy'")
    if [ "${src_copy}" -ne 0 ]; then
        echo "FAIL ${table}.${column}: source still has ${src_copy} -copy rows"
        exit 1
    fi

    local count=0
    local caught_up=0
    for _ in $(seq 1 180); do
        count=$(psql -d destination -tAc "SELECT COUNT(*) FROM ${table} WHERE ${column} LIKE '%-copy'")
        if [ "${count}" -eq 0 ]; then
            caught_up=1
            break
        fi
        sleep 1
    done

    if [ "${caught_up}" -ne 1 ]; then
        echo "FAIL ${table}.${column}: destination still has ${count} rows ending in -copy after 180s"
        for port in 15432 15433; do
            PGPASSWORD=pgdog psql -h 127.0.0.1 -p ${port} -U pgdog -d postgres -c \
                "SELECT slot_name, active, confirmed_flush_lsn, pg_current_wal_lsn() - confirmed_flush_lsn AS lag_bytes FROM pg_replication_slots" || true
        done
        exit 1
    fi
    echo "${table}.${column}: replication caught up"
}

wait_for_no_copy_rows tenants name
wait_for_no_copy_rows accounts full_name
wait_for_no_copy_rows projects name
wait_for_no_copy_rows tasks title
wait_for_no_copy_rows task_comments body
wait_for_no_copy_rows settings name

check_row_count_matches() {
    local table="$1"
    local source_count
    local destination_count

    source_count=$(psql -d source -tAc "SELECT COUNT(*) FROM ${table}")
    destination_count=$(psql -d destination -tAc "SELECT COUNT(*) FROM ${table}")

    if [ "${source_count}" -ne "${destination_count}" ]; then
        echo "MISMATCH ${table}: source=${source_count} destination=${destination_count}"
        exit 1
    fi

    echo "OK ${table}: ${source_count} rows"
}

check_row_count_matches tenants
check_row_count_matches accounts
check_row_count_matches projects
check_row_count_matches tasks
check_row_count_matches task_comments
check_row_count_matches settings

kill -TERM ${PGDOG_PID}
docker compose down
