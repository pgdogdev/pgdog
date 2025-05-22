#!/bin/bash
set -e
PRIMARY="primary"

export PGHOST=${PRIMARY}
export PGPOST=5432
export PGUSER=postgres
export PGPASSWORD=postgres
export PGDATABASE=postgres

PRIMARY_CONN="postgres://postgres:postgres@${PRIMARY}:5432/postgres"
DATA_DIR="/var/lib/postgresql/data"
REPLICA_DIR="/var/lib/postgresql/replica"

while ! pg_isready; do
    sleep 1
done

function shutdown() {
    exit 0
}

# echo "Removing old data directory"
# pg_ctl -D ${DATA_DIR} stop
# rm -f ${DATA_DIR}/postmaster.pid

while true; do
    rm -rf ${REPLICA_DIR}
    mkdir -p ${REPLICA_DIR}
    chmod 750 ${REPLICA_DIR}

    echo "Copying primary data directory"
    if pg_basebackup -D ${REPLICA_DIR}; then
        touch ${REPLICA_DIR}/standby.signal
        break
    fi
done

echo "primary_conninfo = '${PRIMARY_CONN}'" >> ${REPLICA_DIR}/postgresql.auto.conf
echo "shared_preload_libraries = 'pg_stat_statements'" >> ${REPLICA_DIR}/postgresql.auto.conf

echo "Starting replica"
pg_ctl -D ${REPLICA_DIR} start || true
trap shutdown SIGTERM
trap shutdown SIGINT
sleep infinity
