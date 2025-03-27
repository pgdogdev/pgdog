#!/bin/bash

function wait_for_pgdog() {
    echo "Waiting for PgDog"
    while ! pg_isready -h 127.0.0.1 -p 6432 -U pgdog -d pgdog > /dev/null; do
        echo "waiting for PgDog" > /dev/null
    done
    echo "PgDog is ready"
}


function run_pgdog() {
    pushd ${SCRIPT_DIR}/../../
    cargo build
    target/debug/pgdog --config integration/pgdog.toml --users integration/users.toml > ${SCRIPT_DIR}/log.txt &
    PID=$!
    popd
}

function stop_pgdog() {
    kill -TERM ${PID}
    cat ${SCRIPT_DIR}/log.txt
    rm ${SCRIPT_DIR}/log.txt
}
