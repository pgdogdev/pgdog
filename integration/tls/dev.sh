#!/bin/bash
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
HOST=127.0.0.1
PORT=6432
DB=pgdog
USER_A=tls_user_a
USER_B=tls_user_b

run_psql() {
    psql "host=$HOST port=$PORT dbname=$DB user=$1 sslmode=require sslcert=$DIR/$2.crt sslkey=$DIR/$2.key" -c "SELECT 1" > /dev/null 2>&1
}

if [ "${1:-}" = "--source-only" ]; then
    return 0 2>/dev/null || exit 0
fi

PASS=0
FAIL=0

echo "=== TLS client certificate tests ==="

# Test 1: user identity pgdog with pgdog cert (should succeed)
echo -n "$USER_A identity pgdog + pgdog cert: "
if run_psql "$USER_A" client; then
    echo "OK"
    PASS=$((PASS + 1))
else
    echo "FAIL (expected success)"
    FAIL=$((FAIL + 1))
fi

# Test 2: user identity pgdog2 with pgdog2 cert (should succeed)
echo -n "$USER_B identity pgdog2 + pgdog2 cert: "
if run_psql "$USER_B" client2; then
    echo "OK"
    PASS=$((PASS + 1))
else
    echo "FAIL (expected success)"
    FAIL=$((FAIL + 1))
fi

# Test 3: user identity pgdog with pgdog2 cert (should fail)
echo -n "$USER_A identity pgdog + pgdog2 cert: "
if run_psql "$USER_A" client2; then
    echo "FAIL (expected rejection)"
    FAIL=$((FAIL + 1))
else
    echo "OK (rejected)"
    PASS=$((PASS + 1))
fi

# Test 4: user identity pgdog2 with pgdog cert (should fail)
echo -n "$USER_B identity pgdog2 + pgdog cert: "
if run_psql "$USER_B" client; then
    echo "FAIL (expected rejection)"
    FAIL=$((FAIL + 1))
else
    echo "OK (rejected)"
    PASS=$((PASS + 1))
fi

# Test 5: no TLS at all (should fail)
echo -n "no TLS: "
if psql "host=$HOST port=$PORT dbname=$DB user=$USER_A sslmode=disable" -c "SELECT 1" > /dev/null 2>&1; then
    echo "FAIL (expected rejection)"
    FAIL=$((FAIL + 1))
else
    echo "OK (rejected)"
    PASS=$((PASS + 1))
fi

echo ""
echo "Results: $PASS passed, $FAIL failed"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
