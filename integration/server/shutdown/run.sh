#!/bin/bash
set -e
set -m
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../../common.sh
export PGHOST=127.0.0.1 PGUSER=pgdog PGPASSWORD=pgdog
PGDOG_PORT=6432

NAME=pgdog_shutdown

# Must match shutdown_timeout in integration/server/pgdog.toml, in seconds.
SHUTDOWN_TIMEOUT=5
# Limit for steps that normally take less than a second, so a broken step fails and does not hang.
STEP_TIMEOUT=30

function sql() {
    psql -p 5432 -d "$1" -qAtX -v ON_ERROR_STOP=1 -c "$2"
}

function admin() {
    psql -p ${PGDOG_PORT} -U admin admin -qAtX -c "$1" > /dev/null
}

function fail() {
    echo "FAIL ${CASE}: $*"
    exit 1
}

function wait_until() {
    for _ in $(seq $((STEP_TIMEOUT * 10))); do
        eval "$1" && return 0
        sleep 0.1
    done
    return 1
}

function expect_alive() {
    sleep $1
    kill -0 ${PID} 2> /dev/null || fail "pgdog exited after ${1}s"
}

function expect_exit() {
    for _ in $(seq $(($1 * 10))); do
        kill -0 ${PID} 2> /dev/null || break
        sleep 0.1
    done
    kill -0 ${PID} 2> /dev/null && fail "pgdog did not exit within ${1}s"
    wait ${PID} || fail "pgdog exited with code $?"
}

function start() {
    if [ -f ${COMMON_DIR}/pgdog.pid ] && kill -0 $(cat ${COMMON_DIR}/pgdog.pid) 2> /dev/null; then
        fail "pgdog is already running, stop it first"
    fi
    run_pgdog integration/server
    wait_for_pgdog
    PID=$(cat ${COMMON_DIR}/pgdog.pid)
}

function stop() {
    if [ "$1" = SHUTDOWN ]; then
        admin SHUTDOWN
    else
        kill -$1 ${PID}
    fi
}

function hold() {
    coproc CLIENTS { python ${SCRIPT_DIR}/clients.py; }
    CLIENTS_IN=${CLIENTS[1]}
    CLIENTS_OUT=${CLIENTS[0]}
    CLIENTS_JOB=${CLIENTS_PID}
    local line
    read -t ${STEP_TIMEOUT} -u ${CLIENTS[0]} line || true
    [ "${line}" = held ] || fail "transactions were not opened"
}

function clients() {
    echo "$1" >&${CLIENTS_IN}
    if [ "$1" = check ]; then
        local line
        read -t ${STEP_TIMEOUT} -u ${CLIENTS_OUT} line || true
        [ "${line}" = ok ] || fail "clients failed: $1"
    else
        wait ${CLIENTS_JOB} || fail "clients failed: $1"
    fi
}

function expect_admin() {
    admin "SHOW CLIENTS" || fail "admin database does not respond during shutdown"
}

function expect_rejected() {
    local database output
    for database in pgdog pgdog_sharded; do
        output=$(psql -p ${PGDOG_PORT} ${database} -qAtX -c 'SELECT 1' 2>&1) \
            && fail "new connection to ${database} was accepted during shutdown"
        [[ "${output}" == *"PgDog is shutting down"* ]] \
            || fail "new connection to ${database} was not rejected with the shutdown error: ${output}"
    done
}

function replication_objects() {
    sql pgdog "SELECT (SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '${NAME}%') \
        + (SELECT count(*) FROM pg_replication_origin WHERE roname LIKE '__pgdog_origin_${NAME}%')"
}

function create_tables() {
    for database in pgdog shard_0 shard_1; do
        sql ${database} "CREATE SCHEMA ${NAME}; \
            CREATE TABLE ${NAME}.clients (id BIGINT PRIMARY KEY, client INT NOT NULL)"
    done
}

function start_replication() {
    sql pgdog "CREATE TABLE ${NAME}.items (id BIGINT PRIMARY KEY); \
        CREATE PUBLICATION ${NAME} FOR TABLE ${NAME}.items"
    for shard in shard_0 shard_1; do
        sql ${shard} "CREATE TABLE ${NAME}.items (id BIGINT PRIMARY KEY)"
    done
    admin "REPLICATE pgdog pgdog_sharded ${NAME} ${NAME}"
    wait_until '[ "$(replication_objects)" = 3 ]' || fail "replication did not start"
}

function active_slots() {
    sql pgdog "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '${NAME}%' AND active"
}

function cleanup() {
    wait_until '[ "$(active_slots)" = 0 ]' || return 1
    sql pgdog "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE '${NAME}%'; \
        SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin WHERE roname LIKE '__pgdog_origin_${NAME}%'; \
        DROP PUBLICATION IF EXISTS ${NAME}" > /dev/null || return 1
    for database in pgdog shard_0 shard_1; do
        sql ${database} "DROP SCHEMA IF EXISTS ${NAME} CASCADE" || return 1
    done
}

function graceful() {
    start_replication
    hold
    stop $1
    # Transactions are still open, so pgdog must keep draining. Stay below the
    # timeout, or pgdog closes the transactions.
    expect_alive 1
    expect_admin
    expect_rejected
    clients release
    expect_exit ${STEP_TIMEOUT}
    [ "$(replication_objects)" = 0 ] || fail "replication was not cleaned up"
}

function shutdown_timeout() {
    hold
    stop INT
    # Before the timeout, the open transactions must still work.
    sleep $((SHUTDOWN_TIMEOUT - 1))
    clients check
    expect_admin
    expect_rejected
    # After the timeout, pgdog must exit and close the open transactions.
    expect_exit ${STEP_TIMEOUT}
    clients terminated
}

active_venv

export PGDOG_STOP_TRAP=1
trap "stop_pgdog; cleanup > /dev/null 2>&1" EXIT
cleanup > /dev/null 2>&1 || true

for CASE in "graceful INT" "graceful TERM" "graceful SHUTDOWN" "shutdown_timeout"; do
    create_tables
    start
    ${CASE}
    cleanup > /dev/null || fail "cleanup failed"
    echo "PASS ${CASE}"
done
