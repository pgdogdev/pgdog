#!/bin/bash
set -e
DATA_DIR=/var/lib/postgresql/data

# Enable replication connection
echo "host replication all all scram-sha-256" >> ${DATA_DIR}/pg_hba.conf
pg_ctl -D ${DATA_DIR} reload
