#!/bin/bash
#
# N.B.: Scripts using this are expected to define $SCRIPT_DIR
#       correctly.
#
function wait_for_pgdog() {
    echo "Waiting for PgDog"
    while ! pg_isready -h 127.0.0.1 -p 6432 -U pgdog -d pgdog > /dev/null; do
        echo "waiting for PgDog" > /dev/null
    done
    echo "PgDog is ready"
}


function run_pgdog() {
    # We expect all test scripts to define $SCRIPT_DIR.
    pushd ${SCRIPT_DIR}/../../
    # Testing in release is faster
    # and a more reliable test of what happens
    # in prod.
    cargo build --release
    target/release/pgdog \
        --config integration/pgdog.toml \
        --users integration/users.toml \
        > ${SCRIPT_DIR}/log.txt &
    popd
}

function stop_pgdog() {
    killall -TERM pgdog 2> /dev/null || true
    cat ${SCRIPT_DIR}/log.txt
    rm ${SCRIPT_DIR}/log.txt
}

function start_toxi() {
    ./toxiproxy-server > /dev/null &
}

function stop_toxi() {
    killall -TERM toxiproxy-server
}

function active_venv() {
    pushd ${SCRIPT_DIR}/../python
    source venv/bin/activate
    popd
}
