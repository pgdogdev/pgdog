#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

# Force backend Postgres to require TLS in GitHub CI so we exercise the
# client<->PgDog<->Postgres path end to end. Skipped locally because dev
# clusters aren't guaranteed to have server certs configured.
if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
    psql -c "ALTER SYSTEM SET ssl TO on"

    # Give Postgres the suite's server certificate and CA so backend/run.sh
    # can test mutual TLS (PgDog presenting per-database client certs).
    # Postgres requires the key to be owned by it and not world-readable.
    PG_CERTS=/var/lib/postgresql/pgdog-tls
    sudo mkdir -p "${PG_CERTS}"
    sudo cp ${SCRIPT_DIR}/server.crt ${SCRIPT_DIR}/server.key ${SCRIPT_DIR}/ca.crt "${PG_CERTS}/"
    sudo chown -R postgres:postgres "${PG_CERTS}"
    sudo chmod 600 "${PG_CERTS}/server.key"
    psql -c "ALTER SYSTEM SET ssl_cert_file TO '${PG_CERTS}/server.crt'"
    psql -c "ALTER SYSTEM SET ssl_key_file TO '${PG_CERTS}/server.key'"
    psql -c "ALTER SYSTEM SET ssl_ca_file TO '${PG_CERTS}/ca.crt'"

    # TLS connections to pgdog_mtls must present a client certificate signed
    # by the suite CA. Scoped to that database (first matching pg_hba line
    # wins) so the client-side tests in dev.sh, whose backend connections
    # present no certificate, are unaffected.
    psql -c "CREATE DATABASE pgdog_mtls" 2> /dev/null || true
    HBA_FILE=$(psql -tAc "SHOW hba_file")
    if ! sudo grep -q "pgdog_mtls" "${HBA_FILE}"; then
        sudo sed -i '1i hostssl pgdog_mtls all 127.0.0.1/32 scram-sha-256 clientcert=verify-ca' "${HBA_FILE}"
    fi

    PSQL_VERSION=$(psql -tAc "SELECT current_setting('server_version_num')::int / 10000")
    sudo pg_ctlcluster "${PSQL_VERSION}" main restart
fi

run_pgdog integration/tls

# psql requires private keys to be 0600 (git doesn't preserve this).
chmod 600 ${SCRIPT_DIR}/*.key

# pg_isready doesn't present a client cert, so use run_psql from dev.sh instead.
source ${SCRIPT_DIR}/dev.sh --source-only

echo "Waiting for PgDog"
PID_FILE="${SCRIPT_DIR}/../pgdog.pid"
PID=""
if [ -f "${PID_FILE}" ]; then
    PID=$(cat "${PID_FILE}")
fi
while ! run_psql tls_user_a client; do
    if [ -n "${PID}" ] && ! kill -0 "${PID}" 2> /dev/null; then
        echo "PgDog process (pid ${PID}) exited before becoming ready"
        exit 1
    fi
    sleep 0.1
done
echo "PgDog is ready"

bash ${SCRIPT_DIR}/dev.sh

# Backend mTLS tests need the Postgres client-cert setup above (CI only).
if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
    bash ${SCRIPT_DIR}/backend/run.sh
fi

stop_pgdog
