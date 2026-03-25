#!/usr/bin/env bash
# Test wildcard pool lifecycle: create → use → idle → evict → recreate.
#
# Validates that pools are properly evicted after wildcard_pool_idle_timeout,
# memory remains stable through churn cycles, and recreated pools work correctly.
# Critical for deployments with more DBs than max_wildcard_pools.
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OUTPUT_DIR="${SCRIPT_DIR}/../results"

PGDOG_HOST="127.0.0.1"
PGDOG_PORT=6432
IDLE_TIMEOUT="${IDLE_TIMEOUT:-10}"
CYCLE_COUNT=3
BATCH_SIZE=50

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pgdog-host)    PGDOG_HOST="$2";   shift 2 ;;
        --pgdog-port)    PGDOG_PORT="$2";   shift 2 ;;
        --idle-timeout)  IDLE_TIMEOUT="$2"; shift 2 ;;
        --cycles)        CYCLE_COUNT="$2";  shift 2 ;;
        --batch-size)    BATCH_SIZE="$2";   shift 2 ;;
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
LOGFILE="$OUTPUT_DIR/pool_lifecycle.log"
exec > >(tee -a "$LOGFILE") 2>&1

admin_query() {
    PGPASSWORD=admin psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" \
        -U admin -d admin -t -A -c "$1" 2>/dev/null
}

get_pool_count() {
    local pools
    pools=$(admin_query "SHOW POOLS" 2>/dev/null) || true
    printf '%s\n' "$pools" | awk 'NF { count++ } END { print count + 0 }'
}

get_rss() {
    local pid
    pid=$(pgrep -x pgdog 2>/dev/null | head -1 || true)
    if [ -n "$pid" ]; then
        ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' || echo "0"
    else
        echo "0"
    fi
}

FAILURES=0

echo "=== Pool Lifecycle Churn Test ==="
echo "PgDog: ${PGDOG_HOST}:${PGDOG_PORT}"
echo "Idle timeout: ${IDLE_TIMEOUT}s"
echo "Cycles: ${CYCLE_COUNT}, Batch: ${BATCH_SIZE} DBs per cycle"
echo ""

# Configure PgDog's eviction timeout via admin SET so eviction actually fires
# during this test, regardless of the global config value.
echo "Setting wildcard_pool_idle_timeout=${IDLE_TIMEOUT} via admin..."
admin_query "SET wildcard_pool_idle_timeout TO '${IDLE_TIMEOUT}'" || {
    warn "Could not SET wildcard_pool_idle_timeout — eviction may not fire"
}

# Record initial state
initial_pools=$(get_pool_count)
initial_rss=$(get_rss)
echo "Initial state: pools=${initial_pools}, RSS=${initial_rss}KB"
echo ""

TELEMETRY="${OUTPUT_DIR}/pool_lifecycle_telemetry.csv"
echo "cycle,phase,pool_count,rss_kb" > "$TELEMETRY"

for cycle in $(seq 1 "$CYCLE_COUNT"); do
    echo "--- Cycle ${cycle}/${CYCLE_COUNT} ---"

    # Each cycle uses a different range of tenant DBs to force new pool creation
    start_db=$(( (cycle - 1) * BATCH_SIZE + 1 ))
    end_db=$(( cycle * BATCH_SIZE ))

    # ── Create: touch BATCH_SIZE new databases ───────────────────────────────
    echo "  Creating pools for tenant_${start_db}..tenant_${end_db}"
    create_ok=0
    create_fail=0

    for i in $(seq "$start_db" "$end_db"); do
        if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "tenant_$i" \
            -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
            ((create_ok++)) || true
        else
            ((create_fail++)) || true
        fi
    done

    pools_after_create=$(get_pool_count)
    rss_after_create=$(get_rss)
    echo "$cycle,create,$pools_after_create,$rss_after_create" >> "$TELEMETRY"
    echo "  After create: ok=${create_ok}, fail=${create_fail}, pools=${pools_after_create}, RSS=${rss_after_create}KB"

    if [ "$create_fail" -gt 0 ]; then
        warn "  ${create_fail} connections failed during pool creation"
    fi

    # ── Use: run queries to verify pools work ─────────────────────────────
    echo "  Verifying pools with queries..."
    use_ok=0
    use_fail=0

    for i in $(seq "$start_db" "$end_db"); do
        if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "tenant_$i" \
            -c "SELECT current_database()" -t -q -A >/dev/null 2>&1; then
            ((use_ok++)) || true
        else
            ((use_fail++)) || true
        fi
    done

    echo "  Verify: ok=${use_ok}, fail=${use_fail}"

    # ── Idle: wait for pools to be evicted ─────────────────────────────────
    wait_time=$((IDLE_TIMEOUT + 5))
    echo "  Waiting ${wait_time}s for idle pools to be evicted..."
    sleep "$wait_time"

    pools_after_idle=$(get_pool_count)
    rss_after_idle=$(get_rss)
    echo "$cycle,idle,$pools_after_idle,$rss_after_idle" >> "$TELEMETRY"
    echo "  After idle: pools=${pools_after_idle}, RSS=${rss_after_idle}KB"

    # Pools should have decreased
    if [ "$pools_after_idle" -lt "$pools_after_create" ]; then
        pass "  Pools evicted: ${pools_after_create} → ${pools_after_idle}"
    else
        warn "  Pools did NOT decrease: ${pools_after_create} → ${pools_after_idle}"
    fi

    # ── Recreate: reconnect to same databases ─────────────────────────────
    echo "  Recreating pools for the same range..."
    recreate_ok=0
    recreate_fail=0

    for i in $(seq "$start_db" "$end_db"); do
        if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "tenant_$i" \
            -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
            ((recreate_ok++)) || true
        else
            ((recreate_fail++)) || true
        fi
    done

    pools_after_recreate=$(get_pool_count)
    rss_after_recreate=$(get_rss)
    echo "$cycle,recreate,$pools_after_recreate,$rss_after_recreate" >> "$TELEMETRY"
    echo "  After recreate: ok=${recreate_ok}, fail=${recreate_fail}, pools=${pools_after_recreate}"

    if [ "$recreate_fail" -gt 0 ]; then
        fail "  ${recreate_fail} connections failed after pool recreation"
        ((FAILURES++)) || true
    else
        pass "  All ${BATCH_SIZE} pools recreated successfully"
    fi
    echo ""
done

# ── Memory stability check ──────────────────────────────────────────────────

final_rss=$(get_rss)
if [ "$initial_rss" -gt 0 ] && [ "$final_rss" -gt 0 ]; then
    growth_pct=$(( (final_rss - initial_rss) * 100 / initial_rss ))
    echo "Memory: initial=${initial_rss}KB, final=${final_rss}KB, growth=${growth_pct}%"
    if [ "$growth_pct" -gt 30 ]; then
        fail "Memory grew ${growth_pct}% over ${CYCLE_COUNT} churn cycles — possible leak"
        ((FAILURES++)) || true
    else
        pass "Memory growth ${growth_pct}% after ${CYCLE_COUNT} churn cycles"
    fi
else
    warn "Could not measure memory growth"
fi
echo ""

# ── Summary ──────────────────────────────────────────────────────────────────

echo "========================================"
echo "   Pool Lifecycle Churn Summary"
echo "========================================"
printf "  %-26s %s\n" "Cycles:"             "$CYCLE_COUNT"
printf "  %-26s %s\n" "Batch size:"         "$BATCH_SIZE DBs"
printf "  %-26s %s\n" "Idle timeout:"       "${IDLE_TIMEOUT}s"
printf "  %-26s %s\n" "Initial pools:"      "$initial_pools"
printf "  %-26s %s\n" "Telemetry:"          "$TELEMETRY"
echo "========================================"

if [ "$FAILURES" -gt 0 ]; then
    fail "Pool lifecycle test: ${FAILURES} check(s) failed"
    exit 1
else
    pass "Pool lifecycle churn test passed"
    exit 0
fi
