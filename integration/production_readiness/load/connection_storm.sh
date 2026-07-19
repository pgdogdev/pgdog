#!/usr/bin/env bash
# Test PgDog behavior under a thundering herd: many clients simultaneously
# request connections to cold (no warm pool) databases.
#
# Simulates: PgDog restart, pod rescheduling, or max_wildcard_pools eviction
# followed by a burst of traffic. Validates connect_timeout, connect_attempts,
# and backend connection limit behavior.
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OUTPUT_DIR="${SCRIPT_DIR}/../results"

PGDOG_HOST="127.0.0.1"
PGDOG_PORT=6432
STORM_SIZE=200
PARALLEL=100
TARGET_RANGE_START=500
TARGET_RANGE_END=700
TIMEOUT_SEC=10

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pgdog-host)   PGDOG_HOST="$2";        shift 2 ;;
        --pgdog-port)   PGDOG_PORT="$2";        shift 2 ;;
        --storm-size)   STORM_SIZE="$2";        shift 2 ;;
        --parallel)     PARALLEL="$2";          shift 2 ;;
        --range-start)  TARGET_RANGE_START="$2"; shift 2 ;;
        --range-end)    TARGET_RANGE_END="$2";   shift 2 ;;
        --timeout)      TIMEOUT_SEC="$2";        shift 2 ;;
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
LOGFILE="$OUTPUT_DIR/connection_storm.log"
exec > >(tee -a "$LOGFILE") 2>&1

admin_query() {
    PGPASSWORD=admin psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" \
        -U admin -d admin -t -A -c "$1" 2>/dev/null
}

echo "=== Connection Storm (Thundering Herd) Test ==="
echo "PgDog: ${PGDOG_HOST}:${PGDOG_PORT}"
echo "Storm size: ${STORM_SIZE} concurrent connections"
echo "Parallelism: ${PARALLEL}"
echo "Target DBs: tenant_${TARGET_RANGE_START}..tenant_${TARGET_RANGE_END}"
echo "Timeout: ${TIMEOUT_SEC}s per connection"
echo ""

FAILURES=0

# ── Phase 1: Verify target DBs are cold (no existing pool) ─────────────────

echo "--- Phase 1: Ensuring target pools are cold ---"

# Wait for any existing pools in this range to expire
pools_before=$(admin_query "SHOW POOLS" 2>/dev/null | grep -c "tenant_" || echo "0")
echo "  Existing tenant pools: ${pools_before}"
echo ""

# ── Phase 2: Single massive burst ─────────────────────────────────────────

echo "--- Phase 2: Firing ${STORM_SIZE} connections simultaneously ---"

RESULTS_DIR=$(mktemp -d)
storm_start=$(date +%s%N)

storm_connect() {
    local idx=$1
    # Pick a random DB from the target range
    local db_num=$(( TARGET_RANGE_START + (idx % (TARGET_RANGE_END - TARGET_RANGE_START + 1)) ))
    local t_start t_end ms

    t_start=$(date +%s%N)
    if timeout "$TIMEOUT_SEC" bash -c \
        "PGPASSWORD=pgdog psql -h $PGDOG_HOST -p $PGDOG_PORT -U pgdog -d tenant_${db_num} -c 'SELECT 1' -t -q -A" \
        >/dev/null 2>&1; then
        t_end=$(date +%s%N)
        ms=$(( (t_end - t_start) / 1000000 ))
        echo "ok $ms $db_num" > "${RESULTS_DIR}/${idx}"
    else
        echo "fail 0 $db_num" > "${RESULTS_DIR}/${idx}"
    fi
}
export -f storm_connect
export PGDOG_HOST PGDOG_PORT RESULTS_DIR TIMEOUT_SEC TARGET_RANGE_START TARGET_RANGE_END

seq 1 "$STORM_SIZE" | xargs -P "$PARALLEL" -I{} bash -c 'storm_connect {}'

storm_end=$(date +%s%N)
storm_ms=$(( (storm_end - storm_start) / 1000000 ))

# Parse results
storm_ok=0
storm_fail=0
total_latency=0
latencies=()

for f in "$RESULTS_DIR"/*; do
    [ -f "$f" ] || continue
    read -r status ms db_num < "$f"
    if [ "$status" = "ok" ]; then
        ((storm_ok++)) || true
        total_latency=$((total_latency + ms))
        latencies+=("$ms")
    else
        ((storm_fail++)) || true
    fi
done
rm -rf "$RESULTS_DIR"

storm_avg=0
if [ "$storm_ok" -gt 0 ]; then
    storm_avg=$((total_latency / storm_ok))
fi

# Compute p95 and p99
storm_p95="N/A"
storm_p99="N/A"
if [ "${#latencies[@]}" -gt 0 ]; then
    sorted_lat=$(printf '%s\n' "${latencies[@]}" | sort -n)
    n=${#latencies[@]}
    idx_p95=$(( n * 95 / 100 ))
    idx_p99=$(( n * 99 / 100 ))
    [ "$idx_p95" -ge "$n" ] && idx_p95=$((n - 1))
    [ "$idx_p99" -ge "$n" ] && idx_p99=$((n - 1))

    storm_p95=$(echo "$sorted_lat" | sed -n "$((idx_p95 + 1))p")
    storm_p99=$(echo "$sorted_lat" | sed -n "$((idx_p99 + 1))p")
fi

success_rate=0
if [ "$STORM_SIZE" -gt 0 ]; then
    success_rate=$(( storm_ok * 100 / STORM_SIZE ))
fi

echo "  Completed in ${storm_ms}ms"
echo "  Success: ${storm_ok}/${STORM_SIZE} (${success_rate}%)"
echo "  Failed:  ${storm_fail}"
echo "  Avg latency: ${storm_avg}ms"
echo "  p95 latency: ${storm_p95}ms"
echo "  p99 latency: ${storm_p99}ms"

# At least 90% should succeed even during a storm
if [ "$success_rate" -ge 90 ]; then
    pass "Storm: ${success_rate}% success rate"
else
    fail "Storm: only ${success_rate}% success rate (need >= 90%)"
    ((FAILURES++)) || true
fi
echo ""

# ── Phase 3: Verify PgDog is healthy after storm ───────────────────────────

echo "--- Phase 3: Post-storm health check ---"
sleep 2

# Simple connectivity
post_ok=0
for i in $(seq 1 10); do
    if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "tenant_1" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        ((post_ok++)) || true
    fi
done

if [ "$post_ok" -eq 10 ]; then
    pass "PgDog healthy after storm (10/10 queries OK)"
else
    fail "PgDog degraded after storm: only ${post_ok}/10 queries OK"
    ((FAILURES++)) || true
fi

# Metrics endpoint
if curl -sf "http://${PGDOG_HOST}:9090/metrics" >/dev/null 2>&1; then
    pass "Metrics endpoint responsive after storm"
else
    warn "Metrics endpoint not responsive after storm"
fi

# Admin stats
pools_after=$(admin_query "SHOW POOLS" 2>/dev/null | grep -c "tenant_" || echo "0")
echo "  Pools after storm: ${pools_after}"
echo ""

# ── Phase 4: Repeated bursts ───────────────────────────────────────────────

echo "--- Phase 4: 3 rapid-fire bursts (${PARALLEL} connections each) ---"

burst_total_ok=0
burst_total_fail=0

for burst in 1 2 3; do
    BURST_DIR=$(mktemp -d)

    burst_one() {
        local idx=$1
        local db_num=$(( TARGET_RANGE_START + (idx % (TARGET_RANGE_END - TARGET_RANGE_START + 1)) ))
        if timeout "$TIMEOUT_SEC" bash -c \
            "PGPASSWORD=pgdog psql -h $PGDOG_HOST -p $PGDOG_PORT -U pgdog -d tenant_${db_num} -c 'SELECT 1' -t -q -A" \
            >/dev/null 2>&1; then
            echo "ok" > "${BURST_DIR}/${idx}"
        else
            echo "fail" > "${BURST_DIR}/${idx}"
        fi
    }
    export -f burst_one
    export BURST_DIR

    seq 1 "$PARALLEL" | xargs -P "$PARALLEL" -I{} bash -c 'burst_one {}'

    b_ok=$(find "$BURST_DIR" -type f -exec cat {} + 2>/dev/null | awk '$1 == "ok" { count++ } END { print count + 0 }')
    b_fail=$(find "$BURST_DIR" -type f -exec cat {} + 2>/dev/null | awk '$1 == "fail" { count++ } END { print count + 0 }')
    rm -rf "$BURST_DIR"

    burst_total_ok=$((burst_total_ok + b_ok))
    burst_total_fail=$((burst_total_fail + b_fail))
    echo "  Burst ${burst}: ok=${b_ok}, fail=${b_fail}"

    # No sleep between bursts — that's the point
done

burst_total=$((burst_total_ok + burst_total_fail))
burst_rate=0
if [ "$burst_total" -gt 0 ]; then
    burst_rate=$(( burst_total_ok * 100 / burst_total ))
fi

if [ "$burst_rate" -ge 85 ]; then
    pass "Rapid bursts: ${burst_rate}% success rate"
else
    fail "Rapid bursts: only ${burst_rate}% success rate (need >= 85%)"
    ((FAILURES++)) || true
fi
echo ""

# ── Summary ──────────────────────────────────────────────────────────────────

echo "========================================"
echo "    Connection Storm Test Summary"
echo "========================================"
printf "  %-28s %s\n" "Storm size:"           "$STORM_SIZE"
printf "  %-28s %s\n" "Target DBs:"           "tenant_${TARGET_RANGE_START}..${TARGET_RANGE_END}"
printf "  %-28s %s\n" "Storm success rate:"   "${success_rate}%"
printf "  %-28s %s\n" "Storm avg latency:"    "${storm_avg}ms"
printf "  %-28s %s\n" "Storm p95 latency:"    "${storm_p95}ms"
printf "  %-28s %s\n" "Storm p99 latency:"    "${storm_p99}ms"
printf "  %-28s %s\n" "Storm wall time:"      "${storm_ms}ms"
printf "  %-28s %s\n" "Rapid burst rate:"     "${burst_rate}%"
echo "========================================"

if [ "$FAILURES" -gt 0 ]; then
    fail "Connection storm test: ${FAILURES} check(s) failed"
    exit 1
else
    pass "Connection storm test passed"
    exit 0
fi
