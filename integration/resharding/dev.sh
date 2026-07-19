#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEFAULT_BIN="${SCRIPT_DIR}/../../target/debug/pgdog"
PGDOG_BIN=${PGDOG_BIN:-$DEFAULT_BIN}

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

    kill -TERM "${PGDOG_PID:-}" 2>/dev/null || true
    docker compose down || true

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

kill -TERM ${PGBENCH_PID} 2>/dev/null || true
wait ${PGBENCH_PID} 2>/dev/null || true


replace_copy_with_replicate() {
    local table="$1"
    local column="$2"

    # Capture `UPDATE N` from psql so we can log how many -copy rows were cleaned up.
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

# REPLICATION SENTINEL — must be the last DML issued against the source.
# pgbench uses random(1, 1_000_000_000), so id=0 is reserved for this purpose.
# WAL is ordered: once the sentinel row has propagated to both destination shards,
# every preceding change (including the -copy → -replicate updates above) has too.
# settings is an omni table: pgdog broadcasts the insert to all source shards.
SENTINEL_ID=0
psql source -c "INSERT INTO settings (id, name, value) VALUES (${SENTINEL_ID}, 'sentinel_done', 'sentinel_done')"

echo "Waiting for replication to catch up on both destination shards (sentinel settings.id=${SENTINEL_ID}, timeout 120s)..."
DEADLINE=$((SECONDS + 120))
while true; do
    SENTINEL_0=$(PGPASSWORD=pgdog psql -h 127.0.0.1 -p 15434 -U pgdog -d postgres -tAc \
        "SELECT COUNT(*) FROM settings WHERE id = ${SENTINEL_ID} AND name = 'sentinel_done'" \
        2>/dev/null || echo 0)
    SENTINEL_1=$(PGPASSWORD=pgdog psql -h 127.0.0.1 -p 15435 -U pgdog -d postgres -tAc \
        "SELECT COUNT(*) FROM settings WHERE id = ${SENTINEL_ID} AND name = 'sentinel_done'" \
        2>/dev/null || echo 0)
    [ "${SENTINEL_0}" -eq 1 ] && [ "${SENTINEL_1}" -eq 1 ] && break
    if [ "${SECONDS}" -ge "${DEADLINE}" ]; then
        echo "ERROR: replication sentinel did not reach both destinations within 120s"
        echo "  shard 0 (:15434): ${SENTINEL_0}"
        echo "  shard 1 (:15435): ${SENTINEL_1}"
        for port in 15432 15433; do
            PGPASSWORD=pgdog psql -h 127.0.0.1 -p ${port} -U pgdog -d postgres -c \
                "SELECT slot_name, active, confirmed_flush_lsn, pg_current_wal_lsn() - confirmed_flush_lsn AS lag_bytes FROM pg_replication_slots" || true
        done
        exit 1
    fi
    sleep 1
done
echo "Replication caught up on both shards"

wait_for_no_copy_rows() {
    local table="$1"
    local column="$2"

    # Sentinel poll above guarantees destination has caught up; this is a source sanity check.
    local src_copy
    src_copy=$(psql -d source -tAc "SELECT COUNT(*) FROM ${table} WHERE ${column} LIKE '%-copy'")
    if [ "${src_copy}" -ne 0 ]; then
        echo "FAIL ${table}.${column}: source still has ${src_copy} -copy rows"
        exit 1
    fi
    echo "${table}.${column}: source clean"
}

wait_for_no_copy_rows tenants name
wait_for_no_copy_rows accounts full_name
wait_for_no_copy_rows projects name
wait_for_no_copy_rows tasks title
wait_for_no_copy_rows task_comments body
wait_for_no_copy_rows settings name


# pg_count PORT TABLE — row count via a direct postgres connection (bypasses pgdog).
pg_count() { PGPASSWORD=pgdog psql -h 127.0.0.1 -p "$1" -U pgdog -d postgres -tAc "SELECT COUNT(*) FROM $2"; }

# check_row_count_matches TABLE
# Verifies total row count matches between source (via pgdog) and destination (via pgdog).
# Suitable for sharded tables where pgdog aggregates across all shards.
check_row_count_matches() {
    local table="$1"
    local source_count destination_count

    source_count=$(psql -d source -tAc "SELECT COUNT(*) FROM ${table}")
    destination_count=$(psql -d destination -tAc "SELECT COUNT(*) FROM ${table}")

    if [ "${source_count}" -ne "${destination_count}" ]; then
        echo "MISMATCH ${table}: source=${source_count} destination=${destination_count}"
        exit 1
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

    source_count=$(pg_count 15432 "${table}")
    dest0_count=$(pg_count  15434 "${table}")
    dest1_count=$(pg_count  15435 "${table}")

    if [ "${source_count}" -ne "${dest0_count}" ] || [ "${source_count}" -ne "${dest1_count}" ]; then
        echo "MISMATCH omni ${table}: source=${source_count} dest-0(15434)=${dest0_count} dest-1(15435)=${dest1_count} (expected ${source_count} on each)"
        exit 1
    fi

    echo "OK omni ${table}: ${source_count} rows on each shard"
}

check_row_count_matches tenants
check_row_count_matches accounts
check_row_count_matches projects
check_row_count_matches tasks
check_row_count_matches task_comments
check_omni_each_shard settings

cleanup
