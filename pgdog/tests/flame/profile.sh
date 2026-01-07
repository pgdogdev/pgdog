#!/bin/bash
#
# Utils for performance testing.
#
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export PGDOG_BIN=${SCRIPT_DIR}/../../../target/release/pgdog
export PGPASSWORD=pgdog
export PGUSER=pgdog
export PGDATABASE=pgdog
export PGHOST=127.0.0.1
export PGPORT=6432

if [ ! -f ${PGDOG_BIN} ]; then
    echo "PgDog is not compiled in release mode (target/release/pgdog is missing)"
    echo "Please compile PgDog with:"
    echo
    printf "\tcargo build --release"
    echo
    echo
    exit 1
fi

if ! which samply > /dev/null; then
    echo "Samply profiler is not installed on this system"
    echo "Please install Samply with:"
    echo
    printf "\tcargo install samply"
    echo
    echo
    exit 1
fi

paranoid=$(cat /proc/sys/kernel/perf_event_paranoid)

if [ ! "$paranoid" -eq "-1" ]; then
    echo "\"/proc/sys/kernel/perf_event_paranoid\" need to be set to -1 for sampling profiler to work"
    echo "Please set it manually by running:"
    echo
    printf "\techo '-1' | sudo tee /proc/sys/kernel/perf_event_paranoid"
    echo
    echo
    exit 1
fi

function profile_pgdog() {
    pushd $1
    samply record ${PGDOG_BIN} &
    pid=$!

    while ! pg_isready > /dev/null; do
        sleep 1
    done

    pgbench -i
    pgbench -c 10 -t 1000000 -S -P 1 --protocol extended

    kill -TERM $pid
    popd
}

if [ ! -d "$1" ]; then
    echo "Benchmark $1 doesn't exist."
    echo "Available benchmarks:"
    echo "$(ls -d */)"
    exit 1
fi

profile_pgdog $1
