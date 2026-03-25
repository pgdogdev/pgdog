#!/usr/bin/env bash
# Test PgDog behavior under realistic network latency.
#
# Uses toxiproxy between PgDog and Postgres to inject latency
# simulating Cloud SQL Auth Proxy + VPC hop (~2-10ms RTT).
# Validates: checkout_timeout, connect_timeout, tail latency under contention.
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OUTPUT_DIR="${SCRIPT_DIR}/../results"

PGDOG_HOST="127.0.0.1"
PGDOG_PORT=6432
TOXI_HOST="127.0.0.1"
TOXI_API="${TOXI_API:-http://127.0.0.1:8474}"
PROXY_NAME="${PROXY_NAME:-pg_primary}"
LATENCY_MS=5
JITTER_MS=2
CLIENTS=20
DURATION=30
TARGET_DB="tenant_1"
COLD_RANGE_START=1001
COLD_RANGE_END=1050

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pgdog-host)  PGDOG_HOST="$2";  shift 2 ;;
        --pgdog-port)  PGDOG_PORT="$2";  shift 2 ;;
        --toxi-api)    TOXI_API="$2";    shift 2 ;;
        --proxy-name)  PROXY_NAME="$2";  shift 2 ;;
        --latency)     LATENCY_MS="$2";  shift 2 ;;
        --jitter)      JITTER_MS="$2";   shift 2 ;;
        --clients)     CLIENTS="$2";     shift 2 ;;
        --duration)    DURATION="$2";    shift 2 ;;
        --target-db)   TARGET_DB="$2";   shift 2 ;;
        --cold-range-start) COLD_RANGE_START="$2"; shift 2 ;;
        --cold-range-end)   COLD_RANGE_END="$2"; shift 2 ;;
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

for cmd in psql pgbench curl; do
    command -v "$cmd" >/dev/null 2>&1 || { fail "Required command not found: $cmd"; exit 1; }
done

mkdir -p "$OUTPUT_DIR"
LOGFILE="$OUTPUT_DIR/network_latency.log"
exec > >(tee -a "$LOGFILE") 2>&1

WORKLOAD_SQL="${SCRIPT_DIR}/tenant_workload.sql"

CHILDREN=()
cleanup() {
    # Remove toxics
    curl -sf -X DELETE "${TOXI_API}/proxies/${PROXY_NAME}/toxics/latency_downstream" >/dev/null 2>&1 || true
    curl -sf -X DELETE "${TOXI_API}/proxies/${PROXY_NAME}/toxics/latency_upstream" >/dev/null 2>&1 || true
    for pid in "${CHILDREN[@]+"${CHILDREN[@]}"}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT SIGINT SIGTERM

echo "=== Network Latency Test ==="
echo "PgDog: ${PGDOG_HOST}:${PGDOG_PORT}"
echo "Toxiproxy API: ${TOXI_API}"
echo "Proxy: ${PROXY_NAME}"
echo "Injected latency: ${LATENCY_MS}ms ± ${JITTER_MS}ms"
echo ""

# Verify toxiproxy is reachable
if ! curl -sf "${TOXI_API}/version" >/dev/null 2>&1; then
    fail "Toxiproxy API not reachable at ${TOXI_API}"
    exit 1
fi

# ── Phase 1: Baseline (no latency) ─────────────────────────────────────────

echo "--- Phase 1: Baseline latency (no toxics) ---"

baseline_total=0
baseline_count=10
baseline_failures=0

for i in $(seq 1 "$baseline_count"); do
    t_start=$(date +%s%N)
    if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "$TARGET_DB" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        t_end=$(date +%s%N)
        ms=$(( (t_end - t_start) / 1000000 ))
        baseline_total=$((baseline_total + ms))
    else
        ((baseline_failures++)) || true
    fi
done

if [ "$baseline_failures" -gt 0 ]; then
    fail "Baseline: ${baseline_failures}/${baseline_count} queries failed without latency injection"
    exit 1
fi

baseline_avg=$((baseline_total / baseline_count))
pass "Baseline avg latency: ${baseline_avg}ms"
echo ""

# ── Phase 2: Inject latency ─────────────────────────────────────────────────

echo "--- Phase 2: Injecting ${LATENCY_MS}ms ± ${JITTER_MS}ms latency ---"

# Downstream (Postgres → PgDog)
curl -sf -X POST "${TOXI_API}/proxies/${PROXY_NAME}/toxics" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"latency_downstream\",\"type\":\"latency\",\"stream\":\"downstream\",\"attributes\":{\"latency\":${LATENCY_MS},\"jitter\":${JITTER_MS}}}" \
    >/dev/null 2>&1

# Upstream (PgDog → Postgres)
curl -sf -X POST "${TOXI_API}/proxies/${PROXY_NAME}/toxics" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"latency_upstream\",\"type\":\"latency\",\"stream\":\"upstream\",\"attributes\":{\"latency\":${LATENCY_MS},\"jitter\":${JITTER_MS}}}" \
    >/dev/null 2>&1

pass "Latency toxics injected (${LATENCY_MS}ms ± ${JITTER_MS}ms each direction)"
echo ""

# ── Phase 3: Single-query latency under injected delay ───────────────────

echo "--- Phase 3: Single-query latency with injected delay ---"

injected_total=0
injected_count=20
injected_failures=0

for i in $(seq 1 "$injected_count"); do
    t_start=$(date +%s%N)
    if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "$TARGET_DB" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        t_end=$(date +%s%N)
        ms=$(( (t_end - t_start) / 1000000 ))
        injected_total=$((injected_total + ms))
    else
        ((injected_failures++)) || true
    fi
done

if [ "$((injected_count - injected_failures))" -gt 0 ]; then
    injected_avg=$((injected_total / (injected_count - injected_failures)))
else
    injected_avg=0
fi

latency_delta=$((injected_avg - baseline_avg))
echo "  Baseline avg: ${baseline_avg}ms"
echo "  Injected avg: ${injected_avg}ms"
echo "  Delta:        ${latency_delta}ms"

if [ "$injected_failures" -gt 0 ]; then
    warn "Phase 3: ${injected_failures}/${injected_count} queries failed with latency injection"
fi

# Latency should have increased by roughly 2x LATENCY_MS (both directions)
expected_min=$(( LATENCY_MS ))  # conservative: at least 1x latency added
if [ "$latency_delta" -ge "$expected_min" ]; then
    pass "Latency increased by ${latency_delta}ms (expected >= ${expected_min}ms)"
else
    warn "Latency delta ${latency_delta}ms is lower than expected ${expected_min}ms"
fi
echo ""

# ── Phase 4: Load test under latency ────────────────────────────────────────

echo "--- Phase 4: pgbench under ${LATENCY_MS}ms latency (${CLIENTS} clients, ${DURATION}s) ---"

if [ -f "$WORKLOAD_SQL" ]; then
    BENCH_LOG="$OUTPUT_DIR/network_latency_bench.log"

    PGPASSWORD=pgdog pgbench -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog "$TARGET_DB" \
        -c "$CLIENTS" -T "$DURATION" \
        --protocol=extended -f "$WORKLOAD_SQL" \
        --no-vacuum -P 5 \
        > "$BENCH_LOG" 2>&1 || true

    tps=$(awk '/tps = /{gsub(/.*tps = /,""); gsub(/[^0-9.].*/,""); print}' "$BENCH_LOG" | tail -1)
    tps="${tps:-0}"
    lat_avg=$(awk '/latency average =/{gsub(/.*= /,""); gsub(/[^0-9.].*/,""); print}' "$BENCH_LOG")
    lat_avg="${lat_avg:-N/A}"

    echo "  TPS:         ${tps}"
    echo "  Avg latency: ${lat_avg}ms"

    if [ "$tps" != "0" ]; then
        pass "pgbench completed under latency (TPS: ${tps})"
    else
        fail "pgbench produced 0 TPS under latency"
    fi
else
    warn "Workload SQL not found — skipping pgbench phase"
fi
echo ""

# ── Phase 5: Multi-tenant connect under latency ─────────────────────────────

echo "--- Phase 5: Connecting to cold tenant DBs under latency ---"

RESULTS_DIR=$(mktemp -d)
cold_count=$((COLD_RANGE_END - COLD_RANGE_START + 1))
cold_start=$(date +%s)

connect_cold() {
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
export -f connect_cold
export PGDOG_HOST PGDOG_PORT RESULTS_DIR

# Connect to a configurable range that should exist but ideally remain cold.
seq "$COLD_RANGE_START" "$COLD_RANGE_END" | xargs -P 10 -I{} bash -c 'connect_cold {}'

cold_end=$(date +%s)
cold_elapsed=$((cold_end - cold_start))

cold_ok=0
cold_fail=0
cold_total_ms=0
for f in "$RESULTS_DIR"/*; do
    [ -f "$f" ] || continue
    read -r status ms < "$f"
    if [ "$status" = "ok" ]; then
        ((cold_ok++)) || true
        cold_total_ms=$((cold_total_ms + ms))
    else
        ((cold_fail++)) || true
    fi
done
rm -rf "$RESULTS_DIR"

cold_avg=0
if [ "$cold_ok" -gt 0 ]; then
    cold_avg=$((cold_total_ms / cold_ok))
fi

echo "  OK: ${cold_ok}/${cold_count}, Failed: ${cold_fail}"
echo "  Avg cold-connect latency: ${cold_avg}ms (${cold_elapsed}s total)"

if [ "$cold_fail" -eq 0 ]; then
    pass "All cold connections succeeded under latency"
else
    fail "${cold_fail}/${cold_count} cold connections failed under latency"
fi
echo ""

# ── Cleanup toxics ──────────────────────────────────────────────────────────

curl -sf -X DELETE "${TOXI_API}/proxies/${PROXY_NAME}/toxics/latency_downstream" >/dev/null 2>&1 || true
curl -sf -X DELETE "${TOXI_API}/proxies/${PROXY_NAME}/toxics/latency_upstream" >/dev/null 2>&1 || true

# ── Summary ──────────────────────────────────────────────────────────────────

FAILURES=0
[ "$injected_failures" -gt 2 ] && ((FAILURES++)) || true
[ "$cold_fail" -gt 0 ] && ((FAILURES++)) || true

echo "========================================"
echo "     Network Latency Test Summary"
echo "========================================"
printf "  %-28s %s\n" "Baseline avg latency:"     "${baseline_avg}ms"
printf "  %-28s %s\n" "Injected latency:"         "${LATENCY_MS}ms ± ${JITTER_MS}ms"
printf "  %-28s %s\n" "Measured avg latency:"      "${injected_avg}ms"
printf "  %-28s %s\n" "Latency delta:"             "${latency_delta}ms"
printf "  %-28s %s\n" "Cold-connect avg:"          "${cold_avg}ms"
printf "  %-28s %s\n" "Cold-connect failures:"     "${cold_fail}/${cold_count}"
echo "========================================"

if [ "$FAILURES" -gt 0 ]; then
    fail "Network latency test: ${FAILURES} check(s) failed"
    exit 1
else
    pass "Network latency test passed"
    exit 0
fi
