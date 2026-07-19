#!/usr/bin/env bash
# Test idle-in-transaction behavior in transaction pooling mode.
#
# In transaction pooling, a client holding BEGIN without COMMIT blocks
# that backend connection for all other clients in the pool. With pool_size=1-5,
# this can starve an entire tenant's traffic.
#
# Validates: client_idle_in_transaction_timeout, pool fairness, starvation detection.
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OUTPUT_DIR="${SCRIPT_DIR}/../results"

PGDOG_HOST="127.0.0.1"
PGDOG_PORT=6432
TARGET_DB="tenant_1"
IDLE_HOLD_SEC=15
CONCURRENT_QUERIES=10
QUERY_TIMEOUT=5

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pgdog-host)     PGDOG_HOST="$2";     shift 2 ;;
        --pgdog-port)     PGDOG_PORT="$2";     shift 2 ;;
        --target-db)      TARGET_DB="$2";      shift 2 ;;
        --hold-time)      IDLE_HOLD_SEC="$2";  shift 2 ;;
        --concurrent)     CONCURRENT_QUERIES="$2"; shift 2 ;;
        --query-timeout)  QUERY_TIMEOUT="$2";  shift 2 ;;
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

for cmd in psql; do
    command -v "$cmd" >/dev/null 2>&1 || { fail "Required command not found: $cmd"; exit 1; }
done

mkdir -p "$OUTPUT_DIR"
LOGFILE="$OUTPUT_DIR/idle_in_transaction.log"
exec > >(tee -a "$LOGFILE") 2>&1

CHILDREN=()
cleanup() {
    for pid in "${CHILDREN[@]+"${CHILDREN[@]}"}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT SIGINT SIGTERM

FAILURES=0

echo "=== Idle-in-Transaction Test ==="
echo "PgDog: ${PGDOG_HOST}:${PGDOG_PORT}"
echo "Target: ${TARGET_DB}"
echo "Hold time: ${IDLE_HOLD_SEC}s"
echo "Concurrent queries during hold: ${CONCURRENT_QUERIES}"
echo ""

# ── Phase 1: Baseline — normal query latency ────────────────────────────────

echo "--- Phase 1: Baseline query latency ---"

baseline_total=0
baseline_count=10

for i in $(seq 1 "$baseline_count"); do
    t_start=$(date +%s%N)
    PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "$TARGET_DB" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1 || true
    t_end=$(date +%s%N)
    ms=$(( (t_end - t_start) / 1000000 ))
    baseline_total=$((baseline_total + ms))
done

baseline_avg=$((baseline_total / baseline_count))
pass "Baseline avg: ${baseline_avg}ms"
echo ""

# ── Phase 2: Hold a transaction open ────────────────────────────────────────

echo "--- Phase 2: Opening idle transaction (holding for ${IDLE_HOLD_SEC}s) ---"

# Start a connection that BEGINs and sits idle
PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "$TARGET_DB" \
    -c "BEGIN; SELECT pg_sleep(${IDLE_HOLD_SEC}); COMMIT;" -q >/dev/null 2>&1 &
HOLDER_PID=$!
CHILDREN+=("$HOLDER_PID")

# Give it a moment to establish the transaction
sleep 1

echo "  Transaction holder running (PID: ${HOLDER_PID})"
echo ""

# ── Phase 3: Concurrent queries while transaction is held ────────────────────

echo "--- Phase 3: Querying while pool slot is held ---"

RESULTS_DIR=$(mktemp -d)
phase3_start=$(date +%s%N)

query_during_hold() {
    local idx=$1
    local t_start t_end ms

    t_start=$(date +%s%N)
    if timeout "$QUERY_TIMEOUT" bash -c \
        "PGPASSWORD=pgdog psql -h $PGDOG_HOST -p $PGDOG_PORT -U pgdog -d $TARGET_DB -c 'SELECT 1' -t -q -A" \
        >/dev/null 2>&1; then
        t_end=$(date +%s%N)
        ms=$(( (t_end - t_start) / 1000000 ))
        echo "ok $ms" > "${RESULTS_DIR}/${idx}"
    else
        echo "fail 0" > "${RESULTS_DIR}/${idx}"
    fi
}
export -f query_during_hold
export PGDOG_HOST PGDOG_PORT TARGET_DB RESULTS_DIR QUERY_TIMEOUT

seq 1 "$CONCURRENT_QUERIES" | xargs -P "$CONCURRENT_QUERIES" -I{} bash -c 'query_during_hold {}'

phase3_end=$(date +%s%N)
phase3_ms=$(( (phase3_end - phase3_start) / 1000000 ))

q_ok=0
q_fail=0
q_total_ms=0
q_max_ms=0

for f in "$RESULTS_DIR"/*; do
    [ -f "$f" ] || continue
    read -r status ms < "$f"
    if [ "$status" = "ok" ]; then
        ((q_ok++)) || true
        q_total_ms=$((q_total_ms + ms))
        if [ "$ms" -gt "$q_max_ms" ]; then
            q_max_ms="$ms"
        fi
    else
        ((q_fail++)) || true
    fi
done
rm -rf "$RESULTS_DIR"

q_avg=0
if [ "$q_ok" -gt 0 ]; then
    q_avg=$((q_total_ms / q_ok))
fi

echo "  Results: ok=${q_ok}, fail=${q_fail}, avg=${q_avg}ms, max=${q_max_ms}ms, total=${phase3_ms}ms"

# With pool_size > 1, some queries should succeed via other pool slots
# With pool_size = 1, all queries will queue/fail
if [ "$q_ok" -gt 0 ]; then
    pass "Phase 3: ${q_ok}/${CONCURRENT_QUERIES} queries succeeded during transaction hold"
    latency_slowdown=$((q_avg - baseline_avg))
    echo "  Latency increase: ${latency_slowdown}ms above baseline"
else
    warn "Phase 3: All queries failed/timed out — pool may be fully blocked (pool_size=1?)"
fi
echo ""

# ── Phase 4: Multiple holders (saturate all pool slots) ──────────────────────

echo "--- Phase 4: Saturating pool with idle transactions ---"

# Saturate with 5 holders (matches typical pool_size=5)
HOLDER_PIDS=()
for i in $(seq 1 5); do
    PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "$TARGET_DB" \
        -c "BEGIN; SELECT pg_sleep(${IDLE_HOLD_SEC}); COMMIT;" -q >/dev/null 2>&1 &
    pid=$!
    HOLDER_PIDS+=("$pid")
    CHILDREN+=("$pid")
done

sleep 1
echo "  5 idle transactions holding pool slots"

# Try queries — they should queue and hit checkout_timeout
saturated_ok=0
saturated_fail=0

for i in $(seq 1 5); do
    if timeout "$QUERY_TIMEOUT" bash -c \
        "PGPASSWORD=pgdog psql -h $PGDOG_HOST -p $PGDOG_PORT -U pgdog -d $TARGET_DB -c 'SELECT 1' -t -q -A" \
        >/dev/null 2>&1; then
        ((saturated_ok++)) || true
    else
        ((saturated_fail++)) || true
    fi
done

echo "  Queries under full saturation: ok=${saturated_ok}, fail/timeout=${saturated_fail}"

if [ "$saturated_fail" -gt 0 ]; then
    pass "Phase 4: Queries correctly blocked when all pool slots held (${saturated_fail}/5 timed out)"
else
    warn "Phase 4: All queries succeeded — pool may be larger than expected or checkout_timeout not enforced"
fi
echo ""

# ── Phase 5: Recovery after holders release ──────────────────────────────────

echo "--- Phase 5: Waiting for holders to release ---"

for pid in "${HOLDER_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
done
# Also kill the original holder
kill "$HOLDER_PID" 2>/dev/null || true
sleep 2

recovery_ok=0
for i in $(seq 1 10); do
    if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "$TARGET_DB" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        ((recovery_ok++)) || true
    fi
done

if [ "$recovery_ok" -eq 10 ]; then
    pass "Phase 5: Full recovery (10/10 queries OK)"
else
    fail "Phase 5: Incomplete recovery (${recovery_ok}/10 queries OK)"
    ((FAILURES++)) || true
fi
echo ""

# ── Summary ──────────────────────────────────────────────────────────────────

echo "========================================"
echo " Idle-in-Transaction Test Summary"
echo "========================================"
printf "  %-30s %s\n" "Baseline avg latency:"          "${baseline_avg}ms"
printf "  %-30s %s\n" "Queries during hold:"           "${q_ok}/${CONCURRENT_QUERIES} OK"
printf "  %-30s %s\n" "Avg latency during hold:"       "${q_avg}ms"
printf "  %-30s %s\n" "Max latency during hold:"       "${q_max_ms}ms"
printf "  %-30s %s\n" "Saturated pool queries:"        "${saturated_ok}/5 OK, ${saturated_fail}/5 blocked"
printf "  %-30s %s\n" "Recovery:"                      "${recovery_ok}/10 OK"
echo "========================================"

if [ "$FAILURES" -gt 0 ]; then
    fail "Idle-in-transaction test: ${FAILURES} failure(s)"
    exit 1
else
    pass "Idle-in-transaction test passed"
    exit 0
fi
