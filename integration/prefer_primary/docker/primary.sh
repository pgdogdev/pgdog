#!/bin/bash
set -e
DATA_DIR=/var/lib/postgresql/data

# Enable replication connection
echo "host replication all all scram-sha-256" >> ${DATA_DIR}/pg_hba.conf
echo "shared_preload_libraries = 'pg_stat_statements'" >> ${DATA_DIR}/postgresql.auto.conf
pg_ctl -D ${DATA_DIR} restart

psql -c 'CREATE EXTENSION pg_stat_statements'
