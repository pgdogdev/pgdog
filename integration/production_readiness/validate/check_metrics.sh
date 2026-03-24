#!/bin/bash
set -euo pipefail

PGDOG_METRICS="${PGDOG_METRICS_URL:-http://127.0.0.1:9090/metrics}"
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

echo "=== OpenMetrics Validation ==="
echo "Endpoint: $PGDOG_METRICS"
echo ""

METRICS=$(curl -sf "$PGDOG_METRICS" 2>/dev/null) || {
    fail "Cannot reach metrics endpoint at $PGDOG_METRICS"
    exit 1
}

echo "$METRICS" > "$OUTPUT_DIR/metrics_snapshot.txt"
pass "Metrics endpoint reachable ($(wc -l <<< "$METRICS" | tr -d ' ') lines)"

POOL_METRICS=$(grep -c "pool" <<< "$METRICS" || true)
if [ "$POOL_METRICS" -gt 0 ]; then
    pass "Pool metrics present ($POOL_METRICS lines)"
else
    fail "No pool metrics found"
fi

CLIENT_METRICS=$(grep -c "client" <<< "$METRICS" || true)
if [ "$CLIENT_METRICS" -gt 0 ]; then
    pass "Client metrics present ($CLIENT_METRICS lines)"
else
    warn "No client metrics found (may be expected if no active clients)"
fi

ERROR_LINES=$(grep -i "error" <<< "$METRICS" || true)
if [ -n "$ERROR_LINES" ]; then
    TOTAL_ERRORS=$(grep -v "^#" <<< "$ERROR_LINES" | awk '{sum += $NF} END {print sum+0}')
    if [ "$TOTAL_ERRORS" -gt 0 ]; then
        warn "Error metrics detected: total=$TOTAL_ERRORS"
    else
        pass "Error metrics present but zero"
    fi
else
    pass "No error metrics (clean state)"
fi

# Validate Prometheus format
if grep -qE "^# (HELP|TYPE) " <<< "$METRICS"; then
    pass "Valid Prometheus/OpenMetrics format (HELP/TYPE headers present)"
else
    fail "Metrics do not appear to be in valid Prometheus format"
fi

echo ""
echo "=== Results ==="
if [ "$FAILURES" -eq 0 ]; then
    pass "All metrics checks passed"
else
    fail "$FAILURES check(s) failed"
fi

exit "$FAILURES"
