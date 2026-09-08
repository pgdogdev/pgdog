#!/bin/bash
set -e
DATA_DIR=/var/lib/postgresql/data

# Server certificate + CA from the suite (staged by run.sh). Postgres
# requires the key to be owned by it and not world-readable, which a host
# mount can't guarantee, so copy into the data dir first.
cp /certs/server.crt /certs/server.key /certs/ca.crt ${DATA_DIR}/
chmod 600 ${DATA_DIR}/server.key

cat >> ${DATA_DIR}/postgresql.auto.conf <<EOF
ssl = on
ssl_cert_file = '${DATA_DIR}/server.crt'
ssl_key_file = '${DATA_DIR}/server.key'
ssl_ca_file = '${DATA_DIR}/ca.crt'
EOF

# First matching line wins: TLS connections to pgdog_mtls must present a
# client certificate signed by the suite CA. Everything else falls through
# to the image's default rules.
sed -i '1i hostssl pgdog_mtls all all scram-sha-256 clientcert=verify-ca' ${DATA_DIR}/pg_hba.conf

pg_ctl -D ${DATA_DIR} restart

psql -c "CREATE ROLE pgdog LOGIN PASSWORD 'pgdog'"
psql -c "CREATE DATABASE pgdog_mtls OWNER pgdog"
