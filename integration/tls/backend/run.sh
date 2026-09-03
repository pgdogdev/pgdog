#!/bin/bash
#
# Backend (PgDog -> Postgres) mTLS tests: per-database client certificate
# overrides and in-place certificate rotation picked up by RELOAD.
#
# Requires the Postgres setup done by ../run.sh (CI only): ssl on with the
# suite's server certificate, and a pg_hba rule requiring TLS connections
# to the pgdog_mtls database to present a client certificate signed by
# ../ca.crt.
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../../common.sh

HOST=127.0.0.1
PORT=6432

export PGPASSWORD=pgdog

db_psql() {
    psql "host=$HOST port=$PORT dbname=$1 user=pgdog" -c "SELECT 1" > /dev/null 2>&1
}

admin_psql() {
    psql -h $HOST -p $PORT -U admin -d admin "$@"
}

# Seed the rotation path with a certificate Postgres does not trust
# (self-signed, not issued by ca.crt). The files must exist before PgDog
# starts: startup validates every database's TLS settings.
ROTATE_DIR="${SCRIPT_DIR}/rotate"
rm -rf "${ROTATE_DIR}"
mkdir -p "${ROTATE_DIR}"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -keyout "${ROTATE_DIR}/client.key" -out "${ROTATE_DIR}/client.crt" \
    -subj "/CN=untrusted" > /dev/null 2>&1

run_pgdog integration/tls/backend

echo "Waiting for PgDog"
PID_FILE="${SCRIPT_DIR}/../../pgdog.pid"
PID=""
if [ -f "${PID_FILE}" ]; then
    PID=$(cat "${PID_FILE}")
fi
while ! db_psql with_client_cert; do
    if [ -n "${PID}" ] && ! kill -0 "${PID}" 2> /dev/null; then
        echo "PgDog process (pid ${PID}) exited before becoming ready"
        exit 1
    fi
    sleep 0.1
done
echo "PgDog is ready"

PASS=0
FAIL=0

echo "=== Backend TLS client certificate tests ==="

# Test 1: per-database client certificate override (should succeed)
echo -n "with_client_cert (per-database override): "
if db_psql with_client_cert; then
    echo "OK"
    PASS=$((PASS + 1))
else
    echo "FAIL (expected success)"
    FAIL=$((FAIL + 1))
fi

# Test 2: [general] has no client certificate (should fail)
echo -n "no_client_cert ([general] fallback, no cert): "
if db_psql no_client_cert; then
    echo "FAIL (expected rejection)"
    FAIL=$((FAIL + 1))
else
    echo "OK (rejected)"
    PASS=$((PASS + 1))
fi

# Test 3: untrusted certificate at the rotation path (should fail)
echo -n "rotated_client_cert (untrusted cert): "
if db_psql rotated_client_cert; then
    echo "FAIL (expected rejection)"
    FAIL=$((FAIL + 1))
else
    echo "OK (rejected)"
    PASS=$((PASS + 1))
fi

# Test 4: swap the trusted certificate in at the same path and RELOAD.
# The connector must be rebuilt from disk; a cache keyed only by path
# would keep presenting the untrusted certificate.
cp "${SCRIPT_DIR}/../client.crt" "${ROTATE_DIR}/client.crt"
cp "${SCRIPT_DIR}/../client.key" "${ROTATE_DIR}/client.key"
admin_psql -c "RELOAD" > /dev/null

echo -n "rotated_client_cert (trusted cert after RELOAD): "
ROTATED_OK=0
for _ in $(seq 1 60); do
    if db_psql rotated_client_cert; then
        ROTATED_OK=1
        break
    fi
    sleep 0.5
done
if [ "${ROTATED_OK}" = "1" ]; then
    echo "OK"
    PASS=$((PASS + 1))
else
    echo "FAIL (expected success after rotation + RELOAD)"
    FAIL=$((FAIL + 1))
fi

# Test 5: the reload must not have leaked a certificate into [general].
echo -n "no_client_cert (still rejected after RELOAD): "
if db_psql no_client_cert; then
    echo "FAIL (expected rejection)"
    FAIL=$((FAIL + 1))
else
    echo "OK (rejected)"
    PASS=$((PASS + 1))
fi

echo ""
echo "Results: $PASS passed, $FAIL failed"

stop_pgdog

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
