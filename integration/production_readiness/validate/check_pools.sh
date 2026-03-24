#!/bin/bash
set -euo pipefail

PGDOG_HOST="${PGDOG_HOST:-127.0.0.1}"
PGDOG_PORT="${PGDOG_PORT:-6432}"
ADMIN_USER="${ADMIN_USER:-admin}"
ADMIN_DB="${ADMIN_DB:-admin}"
OUTPUT_DIR="${OUTPUT_DIR:-$(dirname "$0")/../results}"
mkdir -p "$OUTPUT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

if ! [ -t 1 ]; then
    RED='' GREEN='' YELLOW='' NC=''
fi

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; FAILURES=$((FAILURES + 1)); }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

FAILURES=0

admin_query() {
    PGPASSWORD="${ADMIN_PASSWORD:-admin}" psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" \
        -U "$ADMIN_USER" -d "$ADMIN_DB" -t -A -c "$1" 2>/dev/null
}

echo "=== Admin Pool Validation ==="
echo "PgDog: $PGDOG_HOST:$PGDOG_PORT"
echo ""

echo "--- SHOW POOLS ---"
POOLS=$(admin_query "SHOW POOLS" 2>/dev/null) || {
    fail "Cannot query admin database"
    exit 1
}
echo "$POOLS" > "$OUTPUT_DIR/show_pools.txt"

POOL_COUNT=$(echo "$POOLS" | grep -c "|" || true)
echo "Total pool entries: $POOL_COUNT"

HEALTHY=$(echo "$POOLS" | grep -c "|t|" || echo "0")
UNHEALTHY=$(echo "$POOLS" | grep -c "|f|" || echo "0")

if [ "$POOL_COUNT" -gt 0 ]; then
    pass "Pools exist ($POOL_COUNT entries)"
else
    fail "No pools found"
fi

if [ "$UNHEALTHY" -gt 0 ]; then
    warn "$UNHEALTHY unhealthy pool(s) detected"
else
    pass "All pools healthy"
fi

echo ""
echo "--- SHOW CLIENTS ---"
CLIENTS=$(admin_query "SHOW CLIENTS" 2>/dev/null) || {
    warn "SHOW CLIENTS failed"
    CLIENTS=""
}
echo "$CLIENTS" > "$OUTPUT_DIR/show_clients.txt"

CLIENT_COUNT=$(echo "$CLIENTS" | grep -c "|" || true)
echo "Active clients: $CLIENT_COUNT"

echo ""
echo "--- SHOW STATS ---"
STATS=$(admin_query "SHOW STATS" 2>/dev/null) || {
    warn "SHOW STATS failed"
    STATS=""
}
echo "$STATS" > "$OUTPUT_DIR/show_stats.txt"

echo ""
echo "=== Results ==="
if [ "$FAILURES" -eq 0 ]; then
    pass "All pool checks passed"
else
    fail "$FAILURES check(s) failed"
fi

exit "$FAILURES"
