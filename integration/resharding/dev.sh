#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEFAULT_BIN="${SCRIPT_DIR}/../../target/debug/pgdog"
PGDOG_BIN=${PGDOG_BIN:-$DEFAULT_BIN}

pushd ${SCRIPT_DIR}
docker-compose down && docker-compose up -d

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

    psql source -c "UPDATE ${table} SET ${column} = regexp_replace(${column}, '-copy$', '-replicate') WHERE ${column} LIKE '%-copy';"
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

    while true; do
        count=$(psql -d destination -tAc "SELECT COUNT(*) FROM ${table} WHERE ${column} LIKE '%-copy'")
        if [ "${count}" -eq 0 ]; then
            echo "${table}.${column}: replication caught up"
            break
        fi

        echo "${table}.${column}: waiting for ${count} rows ending in -copy"
        sleep 1
    done
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
docker-compose down
