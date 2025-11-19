#!/bin/bash
#
# Dev CLI.
#

# Connect to the admin database.
function admin() {
    PGPASSWORD=pgdog psql -h 127.0.0.1 -p 6432 -U admin admin
}

# Run pgbench.
#
# Arguments:
#
# - protocol: simple|extended|prepared
#
function bench() {
    PGPASSWORD=pgdog pgbench -h 127.0.0.1 -p 6432 -U pgdog pgdog --protocol ${2:-simple} -t 100000 -c 10 -P 1 -S
}

function bench_init() {
    PGPASSWORD=pgdog pgbench -h 127.0.0.1 -p 6432 -U pgdog pgdog -i
}

function psql_cmd() {
    PGPASSWORD=pgdog psql -h 127.0.0.1 -p 6432 -U pgdog pgdog
}

# Parse command
case "$1" in
    admin)
        admin
        ;;
    psql)
        psql_cmd
        ;;
    binit)
        bench_init
        ;;
    bench)
        bench $2
        ;;
    *)
        echo "Usage: $0 {admin} {bench}"
        exit 1
        ;;
esac
