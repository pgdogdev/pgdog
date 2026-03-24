#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OUTPUT_DIR="${SCRIPT_DIR}/../results"

POOL_SIZE=5
CONNECTIONS=50
PGDOG_HOST="127.0.0.1"
PGDOG_PORT=6432
TARGET_DB="tenant_1"
SLEEP_DURATION=10
PROBE_COUNT=20

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pool-size)   POOL_SIZE="$2";   shift 2 ;;
        --connections) CONNECTIONS="$2"; shift 2 ;;
        --pgdog-host)  PGDOG_HOST="$2"; shift 2 ;;
        --pgdog-port)  PGDOG_PORT="$2"; shift 2 ;;
        --target-db)   TARGET_DB="$2";  shift 2 ;;
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
LOGFILE="$OUTPUT_DIR/pool_pressure.log"
exec > >(tee -a "$LOGFILE") 2>&1

SATURATOR_PIDS=()
cleanup() {
    echo ""
    echo "Cleaning up saturating connections..."
    for pid in "${SATURATOR_PIDS[@]+"${SATURATOR_PIDS[@]}"}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup SIGINT SIGTERM EXIT

echo "=== Pool Pressure Test ==="
echo "Target: ${PGDOG_HOST}:${PGDOG_PORT} / ${TARGET_DB}"
echo "Expected pool_size: ${POOL_SIZE}, Saturating connections: ${CONNECTIONS}"
echo ""
warn "This test assumes PgDog is configured with pool_size=${POOL_SIZE} for ${TARGET_DB}"
echo ""

# ── Phase 1: Saturate the pool ───────────────────────────────────────────────

echo "--- Phase 1: Saturating pool with ${CONNECTIONS} long-running connections ---"
saturate_start=$(date +%s)

for i in $(seq 1 "$CONNECTIONS"); do
    PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "$TARGET_DB" \
        -c "SELECT pg_sleep(${SLEEP_DURATION})" -q >/dev/null 2>&1 &
    SATURATOR_PIDS+=("$!")

    if (( i % 10 == 0 )); then
        echo "  [${i}/${CONNECTIONS}] connections launched"
    fi
done

# Brief pause so connections are established and pools are occupied
sleep 2
echo "  All ${CONNECTIONS} saturating connections launched"
echo ""

# ── Phase 2: Probe while pool is saturated ───────────────────────────────────

echo "--- Phase 2: Probing during saturation (${PROBE_COUNT} queries) ---"

probe_successes=0
probe_timeouts=0
probe_total_ms=0

for i in $(seq 1 "$PROBE_COUNT"); do
    t_start=$(date +%s%N)

    # 3-second timeout: if pool is full these should queue or timeout
    if timeout 3 bash -c "PGPASSWORD=pgdog psql -h $PGDOG_HOST -p $PGDOG_PORT -U pgdog -d $TARGET_DB -c 'SELECT 1' -t -q -A" >/dev/null 2>&1; then
        t_end=$(date +%s%N)
        ms=$(( (t_end - t_start) / 1000000 ))
        ((probe_successes++)) || true
        probe_total_ms=$((probe_total_ms + ms))
        echo "  [${i}/${PROBE_COUNT}] OK (${ms}ms)"
    else
        ((probe_timeouts++)) || true
        echo "  [${i}/${PROBE_COUNT}] TIMEOUT/ERROR"
    fi
done

if [ "$probe_successes" -gt 0 ]; then
    probe_avg=$((probe_total_ms / probe_successes))
else
    probe_avg=0
fi

echo ""
if [ "$probe_timeouts" -gt 0 ]; then
    warn "During saturation: ${probe_successes} succeeded, ${probe_timeouts} timed out (avg ${probe_avg}ms)"
else
    pass "All ${PROBE_COUNT} probes succeeded during saturation (avg ${probe_avg}ms)"
fi
echo ""

# ── Phase 3: Recovery ────────────────────────────────────────────────────────

echo "--- Phase 3: Recovery after releasing saturating connections ---"

for pid in "${SATURATOR_PIDS[@]+"${SATURATOR_PIDS[@]}"}"; do
    kill "$pid" 2>/dev/null || true
done
SATURATOR_PIDS=()

# Wait for connections to fully terminate
sleep 2

recovery_start=$(date +%s%N)
recovery_successes=0
recovery_failures=0

for i in $(seq 1 "$PROBE_COUNT"); do
    if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "$TARGET_DB" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        ((recovery_successes++)) || true
    else
        ((recovery_failures++)) || true
    fi
done

recovery_end=$(date +%s%N)
recovery_ms=$(( (recovery_end - recovery_start) / 1000000 ))

if [ "$recovery_failures" -eq 0 ]; then
    pass "Recovery: all ${PROBE_COUNT} queries succeeded (${recovery_ms}ms total)"
else
    fail "Recovery: ${recovery_failures}/${PROBE_COUNT} queries still failing"
fi
echo ""

# ── Phase 4: Admin stats verification ────────────────────────────────────────

echo "--- Phase 4: Admin stats verification ---"

STATS=$(PGPASSWORD=admin psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U admin -d admin \
    -t -A -c "SHOW STATS" 2>/dev/null) || true

if [ -n "$STATS" ]; then
    echo "$STATS" > "$OUTPUT_DIR/pool_pressure_stats.txt"
    pass "Admin stats collected"
    # Show lines matching the target DB
    target_stats=$(echo "$STATS" | grep "$TARGET_DB" || true)
    if [ -n "$target_stats" ]; then
        echo "  Stats for ${TARGET_DB}:"
        echo "  $target_stats"
    fi
else
    warn "Could not query admin stats"
fi

POOLS=$(PGPASSWORD=admin psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U admin -d admin \
    -t -A -c "SHOW POOLS" 2>/dev/null) || true

if [ -n "$POOLS" ]; then
    echo "$POOLS" > "$OUTPUT_DIR/pool_pressure_pools.txt"
    target_pools=$(echo "$POOLS" | grep "$TARGET_DB" || true)
    if [ -n "$target_pools" ]; then
        echo "  Pools for ${TARGET_DB}:"
        echo "  $target_pools"
    fi
fi
echo ""

# ── Summary ──────────────────────────────────────────────────────────────────

echo "========================================"
echo "      Pool Pressure Test Summary"
echo "========================================"
printf "  %-30s %s\n" "Target database:"              "$TARGET_DB"
printf "  %-30s %s\n" "Configured pool_size:"         "$POOL_SIZE"
printf "  %-30s %s\n" "Saturating connections:"       "$CONNECTIONS"
printf "  %-30s %s\n" "Probes during saturation:"     "$PROBE_COUNT"
printf "  %-30s %s\n" "  Succeeded:"                  "$probe_successes"
printf "  %-30s %s\n" "  Timed out:"                  "$probe_timeouts"
printf "  %-30s %s\n" "  Avg latency (succeeded):"    "${probe_avg}ms"
printf "  %-30s %s\n" "Recovery probes succeeded:"    "${recovery_successes}/${PROBE_COUNT}"
printf "  %-30s %s\n" "Recovery total time:"          "${recovery_ms}ms"
echo "========================================"

if [ "$recovery_failures" -gt 0 ]; then
    fail "Pool did not fully recover"
    exit 1
else
    pass "Pool pressure test passed"
    exit 0
fi
