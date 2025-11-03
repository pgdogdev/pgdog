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
    PGPASSWORD=pgdog pgbench -h 127.0.0.1 -p 6432 -U pgdog pgdog --protocol ${1:-simple}
}

# Parse command
case "$1" in
    admin)
        admin
        ;;
    bench)
        bench $1
        ;;
    *)
        echo "Usage: $0 {admin} {bench}"
        exit 1
        ;;
esac
