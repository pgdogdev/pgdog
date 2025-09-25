#!/bin/bash
#
# N.B.: Scripts using this are expected to define $SCRIPT_DIR
#       correctly.
#
COMMON_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
function wait_for_pgdog() {
    echo "Waiting for PgDog"
    while ! pg_isready -h 127.0.0.1 -p 6432 -U pgdog -d pgdog > /dev/null; do
        echo "waiting for PgDog" > /dev/null
    done
    echo "PgDog is ready"
}


function run_pgdog() {
    # We expect all test scripts to define $SCRIPT_DIR.
    pushd ${COMMON_DIR}/../
    local config_path=${1:-"integration"}
    local binary="${PGDOG_BIN:-}"
    if [ -z "${binary}" ]; then
        # Testing in release is faster and mirrors production.
        cargo build --release
        binary="target/release/pgdog"
    fi
    "${binary}" \
        --config ${config_path}/pgdog.toml \
        --users ${config_path}/users.toml \
        > ${COMMON_DIR}/log.txt &
    popd
}

function stop_pgdog() {
    killall -TERM pgdog 2> /dev/null || true
    cat ${COMMON_DIR}/log.txt
    rm ${COMMON_DIR}/log.txt
}

function start_toxi() {
    ./toxiproxy-server > /dev/null &
}

function stop_toxi() {
    killall -TERM toxiproxy-server
}

function active_venv() {
    pushd ${COMMON_DIR}/python
    source venv/bin/activate
    popd
}
