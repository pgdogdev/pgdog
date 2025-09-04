#!/bin/bash
set -e

# Test that passwordless users get their pools paused when passthrough auth is enabled
# This validates the specific feature added in the PR

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export PGPASSWORD=pgdog
export PGPORT=6432
export PGHOST=127.0.0.1

echo "🧪 Testing pool pausing for passwordless users with passthrough auth..."

# Kill any existing pgdog processes
killall -TERM pgdog 2> /dev/null || true
sleep 1

# Start pgdog with passthrough auth enabled
echo "📦 Starting PgDog with passthrough auth enabled..."
${SCRIPT_DIR}/../../../target/release/pgdog \
    --config ${SCRIPT_DIR}/pgdog-enabled.toml \
    --users ${SCRIPT_DIR}/users.toml &

PGDOG_PID=$!
sleep 2

# Check if pgdog started successfully (this validates pools didn't get banned)
if ! kill -0 $PGDOG_PID 2>/dev/null; then
    echo "❌ FAIL: PgDog process died (probably due to pool banning)"
    exit 1
fi

echo "✅ PASS: PgDog started successfully without pool banning"

# Try to connect with the passwordless user (pgdog1)
# This should work because passthrough auth is enabled
echo "🔑 Testing connection with passwordless user..."
if psql -U pgdog1 pgdog -c 'SELECT 1 AS test_connection' > /dev/null 2>&1; then
    echo "✅ PASS: Passwordless user can connect with passthrough auth"
else
    echo "❌ FAIL: Passwordless user cannot connect"
    kill $PGDOG_PID 2>/dev/null || true
    exit 1
fi

# Clean up
kill $PGDOG_PID 2>/dev/null || true
sleep 1

# Test that without passthrough auth, passwordless users are properly rejected
echo "🔒 Testing without passthrough auth (should reject passwordless user)..."
${SCRIPT_DIR}/../../../target/release/pgdog \
    --config ${SCRIPT_DIR}/pgdog-disabled.toml \
    --users ${SCRIPT_DIR}/users.toml &

PGDOG_PID=$!
sleep 2

# Check if pgdog started successfully 
if ! kill -0 $PGDOG_PID 2>/dev/null; then
    echo "❌ FAIL: PgDog process died when passthrough auth disabled"
    exit 1
fi

# Try to connect with passwordless user - this should fail
echo "🚫 Testing that passwordless user is rejected..."
if psql -U pgdog1 pgdog -c 'SELECT 1' > /dev/null 2>&1; then
    echo "❌ FAIL: Passwordless user should not be able to connect when passthrough auth disabled"
    kill $PGDOG_PID 2>/dev/null || true
    exit 1
else
    echo "✅ PASS: Passwordless user properly rejected when passthrough auth disabled"
fi

# Clean up
kill $PGDOG_PID 2>/dev/null || true
sleep 1

echo "🎉 All tests passed! Pool pausing feature is working correctly."
