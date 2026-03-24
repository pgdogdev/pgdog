#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OUTPUT_DIR="${SCRIPT_DIR}/../results"

COUNT=2000
PGDOG_HOST="127.0.0.1"
PGDOG_PORT=6432
PARALLEL=50

while [[ $# -gt 0 ]]; do
    case "$1" in
        --count)      COUNT="$2";      shift 2 ;;
        --pgdog-host) PGDOG_HOST="$2"; shift 2 ;;
        --pgdog-port) PGDOG_PORT="$2"; shift 2 ;;
        --parallel)   PARALLEL="$2";   shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'
if ! [ -t 1 ]; then
    RED='' GREEN='' YELLOW='' NC=''
fi

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

for cmd in psql curl; do
    command -v "$cmd" >/dev/null 2>&1 || { fail "Required command not found: $cmd"; exit 1; }
done

mkdir -p "$OUTPUT_DIR"
LOGFILE="$OUTPUT_DIR/scale_connect.log"
exec > >(tee -a "$LOGFILE") 2>&1

cleanup() {
    wait 2>/dev/null || true
}
trap cleanup EXIT SIGINT SIGTERM

echo "=== Scale Connection Test ==="
echo "Target: ${PGDOG_HOST}:${PGDOG_PORT}"
echo "Databases: ${COUNT}, Parallelism: ${PARALLEL}"
echo ""

# ── Phase 1: Sequential warm-up ─────────────────────────────────────────────

echo "--- Phase 1: Sequential warm-up (first 10 databases) ---"
WARMUP_COUNT=$((COUNT < 10 ? COUNT : 10))
warmup_total_ms=0
warmup_failures=0

for i in $(seq 1 "$WARMUP_COUNT"); do
    t_start=$(date +%s%N)
    if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "tenant_$i" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        t_end=$(date +%s%N)
        ms=$(( (t_end - t_start) / 1000000 ))
        warmup_total_ms=$((warmup_total_ms + ms))
        echo "  [${i}/${WARMUP_COUNT}] tenant_${i}: ${ms}ms"
    else
        ((warmup_failures++)) || true
        warn "  [${i}/${WARMUP_COUNT}] tenant_${i}: FAILED"
    fi
done

if [ "$warmup_failures" -eq 0 ]; then
    warmup_avg=$((warmup_total_ms / WARMUP_COUNT))
    pass "Warm-up complete: avg ${warmup_avg}ms per connection"
else
    fail "Warm-up had ${warmup_failures} failures out of ${WARMUP_COUNT}"
fi
echo ""

# ── Phase 2: Parallel batch connect ─────────────────────────────────────────

echo "--- Phase 2: Parallel batch connect (${COUNT} databases, ${PARALLEL} at a time) ---"

RESULTS_DIR=$(mktemp -d)
phase2_start=$(date +%s)

connect_one() {
    local idx=$1
    local t_start t_end ms
    t_start=$(date +%s%N)
    if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "tenant_$idx" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        t_end=$(date +%s%N)
        ms=$(( (t_end - t_start) / 1000000 ))
        echo "ok $ms" > "${RESULTS_DIR}/${idx}"
    else
        echo "fail 0" > "${RESULTS_DIR}/${idx}"
    fi
}
export -f connect_one
export PGDOG_HOST PGDOG_PORT RESULTS_DIR

seq 1 "$COUNT" | xargs -P "$PARALLEL" -I{} bash -c 'connect_one {}'

phase2_end=$(date +%s)
phase2_elapsed=$((phase2_end - phase2_start))

successes=0
failures=0
total_latency_ms=0
for f in "$RESULTS_DIR"/*; do
    read -r status ms < "$f"
    if [ "$status" = "ok" ]; then
        ((successes++)) || true
        total_latency_ms=$((total_latency_ms + ms))
    else
        ((failures++)) || true
    fi
done
rm -rf "$RESULTS_DIR"

if [ "$successes" -gt 0 ]; then
    avg_latency=$((total_latency_ms / successes))
else
    avg_latency=0
fi

batch_processed=$((successes + failures))
if (( batch_processed % 200 == 0 || batch_processed == COUNT )); then
    : # progress already implicit from xargs completing
fi

if [ "$failures" -eq 0 ]; then
    pass "All ${COUNT} connections succeeded in ${phase2_elapsed}s (avg ${avg_latency}ms)"
else
    fail "${failures}/${COUNT} connections failed (${successes} succeeded, ${phase2_elapsed}s)"
fi
echo ""

# ── Phase 3: Verify pools via admin DB ───────────────────────────────────────

echo "--- Phase 3: Admin pool verification ---"
pool_count=0

POOLS=$(PGPASSWORD=admin psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U admin -d admin \
    -t -A -c "SHOW POOLS" 2>/dev/null) || true

if [ -n "$POOLS" ]; then
    pool_count=$(echo "$POOLS" | grep -c "|" || true)
    echo "$POOLS" > "$OUTPUT_DIR/scale_connect_pools.txt"
    if [ "$pool_count" -ge "$COUNT" ]; then
        pass "Pool count ${pool_count} >= expected ${COUNT}"
    else
        warn "Pool count ${pool_count} < expected ${COUNT} (pools may be created lazily)"
    fi
else
    warn "Could not query admin database — skipping pool verification"
fi
echo ""

# ── Phase 4: Metrics check ──────────────────────────────────────────────────

echo "--- Phase 4: Metrics endpoint check ---"
metrics_pool_count="N/A"

METRICS=$(curl -sf "http://${PGDOG_HOST}:9090/metrics" 2>/dev/null) || true
if [ -n "$METRICS" ]; then
    echo "$METRICS" > "$OUTPUT_DIR/scale_connect_metrics.txt"
    metrics_pool_count=$(echo "$METRICS" | grep -c "pool" || echo "0")
    pass "Metrics endpoint reachable (${metrics_pool_count} pool-related lines)"
else
    warn "Metrics endpoint unreachable at http://${PGDOG_HOST}:9090/metrics"
fi
echo ""

# ── Summary ──────────────────────────────────────────────────────────────────

echo "========================================"
echo "       Scale Connection Summary"
echo "========================================"
printf "  %-22s %s\n" "Total databases:"    "$COUNT"
printf "  %-22s %s\n" "Successes:"          "$successes"
printf "  %-22s %s\n" "Failures:"           "$failures"
printf "  %-22s %s\n" "Avg latency:"        "${avg_latency}ms"
printf "  %-22s %s\n" "Total time:"         "${phase2_elapsed}s"
printf "  %-22s %s\n" "Pool count (admin):" "$pool_count"
printf "  %-22s %s\n" "Metrics pool lines:" "$metrics_pool_count"
echo "========================================"

if [ "$failures" -gt 0 ]; then
    fail "Scale test completed with ${failures} failures"
    exit 1
else
    pass "Scale test passed"
    exit 0
fi
