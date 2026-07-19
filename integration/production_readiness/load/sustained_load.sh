#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OUTPUT_DIR="${SCRIPT_DIR}/../results"

DURATION_MIN=10
CLIENTS=50
TENANTS=100
PGDOG_HOST="127.0.0.1"
PGDOG_PORT=6432
SAMPLE_INTERVAL=30
GROWTH_THRESHOLD=20

while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration)   DURATION_MIN="$2";  shift 2 ;;
        --clients)    CLIENTS="$2";       shift 2 ;;
        --tenants)    TENANTS="$2";       shift 2 ;;
        --pgdog-host) PGDOG_HOST="$2";    shift 2 ;;
        --pgdog-port) PGDOG_PORT="$2";    shift 2 ;;
        --pgdog-pid)  PGDOG_PID_ARG="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

DURATION_SEC=$((DURATION_MIN * 60))

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

for cmd in psql pgbench curl shuf; do
    command -v "$cmd" >/dev/null 2>&1 || { fail "Required command not found: $cmd"; exit 1; }
done

WORKLOAD_SQL="${SCRIPT_DIR}/tenant_workload.sql"
if [ ! -f "$WORKLOAD_SQL" ]; then
    fail "Workload file not found: ${WORKLOAD_SQL}"
    exit 1
fi

SOAK_DIR="${OUTPUT_DIR}/soak_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$SOAK_DIR/metrics" "$SOAK_DIR/bench"
LOGFILE="${SOAK_DIR}/sustained_load.log"
exec > >(tee -a "$LOGFILE") 2>&1

CHILDREN=()
cleanup() {
    echo ""
    echo "Shutting down..."
    for pid in "${CHILDREN[@]+"${CHILDREN[@]}"}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup SIGINT SIGTERM

echo "=== Sustained Load (Soak) Test ==="
echo "Duration: ${DURATION_MIN} min (${DURATION_SEC}s)"
echo "Clients: ${CLIENTS}, Tenants: ${TENANTS}"
echo "Target: ${PGDOG_HOST}:${PGDOG_PORT}"
echo "Results: ${SOAK_DIR}"
echo ""

# ── Find PgDog PID for memory tracking ───────────────────────────────────────

PGDOG_PID="${PGDOG_PID_ARG:-}"
if [ -z "$PGDOG_PID" ]; then
    PGDOG_PID=$(pgrep -x pgdog 2>/dev/null | head -1 || true)
fi
if [ -n "$PGDOG_PID" ]; then
    echo "Tracking PgDog PID: ${PGDOG_PID}"
else
    warn "Cannot find PgDog process — memory tracking disabled"
fi

# ── Launch pgbench workers ───────────────────────────────────────────────────

TENANT_IDS=$(shuf -i 1-"$TENANTS" -n "$TENANTS" | sort -n)

clients_per_tenant=$((CLIENTS / TENANTS))
if [ "$clients_per_tenant" -lt 1 ]; then
    clients_per_tenant=1
fi
remainder=$((CLIENTS - clients_per_tenant * TENANTS))

echo "--- Launching pgbench across ${TENANTS} tenants ---"
BENCH_PIDS=()

for tid in $TENANT_IDS; do
    c=$clients_per_tenant
    if [ "$remainder" -gt 0 ]; then
        c=$((c + 1))
        ((remainder--)) || true
    fi

    PGPASSWORD=pgdog pgbench -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog "tenant_$tid" \
        -c "$c" -T "$DURATION_SEC" \
        --protocol=extended -f "$WORKLOAD_SQL" \
        --no-vacuum -P 30 \
        > "${SOAK_DIR}/bench/tenant_${tid}.log" 2>&1 &
    pid=$!
    BENCH_PIDS+=("$pid")
    CHILDREN+=("$pid")
done
echo "  ${TENANTS} pgbench processes launched"
echo ""

# ── Periodic telemetry collector ─────────────────────────────────────────────

TELEMETRY_CSV="${SOAK_DIR}/telemetry.csv"
echo "elapsed_s,rss_kb,pool_count,error_lines" > "$TELEMETRY_CSV"

collect_telemetry() {
    local elapsed=0
    while [ "$elapsed" -le "$DURATION_SEC" ]; do
        # RSS
        rss="N/A"
        if [ -n "$PGDOG_PID" ] && kill -0 "$PGDOG_PID" 2>/dev/null; then
            rss=$(ps -o rss= -p "$PGDOG_PID" 2>/dev/null | tr -d ' ' || echo "N/A")
        fi

        # Metrics snapshot
        metrics_file="${SOAK_DIR}/metrics/t${elapsed}.txt"
        curl -sf "http://${PGDOG_HOST}:9090/metrics" > "$metrics_file" 2>/dev/null || true
        error_lines=$(grep -ci "error" "$metrics_file" 2>/dev/null || echo "0")

        # Pool count via admin
        pool_count=$(PGPASSWORD=admin psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" \
            -U admin -d admin -t -A -c "SHOW POOLS" 2>/dev/null | grep -c "|" || echo "0")

        rss_val="${rss}"
        [ "$rss_val" = "N/A" ] && rss_val=0
        echo "${elapsed},${rss_val},${pool_count},${error_lines}" >> "$TELEMETRY_CSV"

        rss_mb="N/A"
        if [ "$rss" != "N/A" ] && [ "$rss" -gt 0 ] 2>/dev/null; then
            rss_mb="$((rss / 1024))MB"
        fi
        printf "  [%5ds/%ds] RSS=%-8s pools=%-5s errors=%s\n" \
            "$elapsed" "$DURATION_SEC" "$rss_mb" "$pool_count" "$error_lines"

        sleep "$SAMPLE_INTERVAL"
        elapsed=$((elapsed + SAMPLE_INTERVAL))
    done
}
collect_telemetry &
TELEMETRY_PID=$!
CHILDREN+=("$TELEMETRY_PID")

# ── Wait for pgbench to complete ─────────────────────────────────────────────

for pid in "${BENCH_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done

kill "$TELEMETRY_PID" 2>/dev/null || true
wait "$TELEMETRY_PID" 2>/dev/null || true

echo ""
echo "--- Load phase complete, analyzing results ---"
echo ""

# ── Analyze telemetry ────────────────────────────────────────────────────────

VERDICT_FAILURES=0

# Memory analysis
rss_values=$(tail -n +2 "$TELEMETRY_CSV" | cut -d',' -f2 | grep -v '^0$' || true)
if [ -n "$rss_values" ]; then
    first_rss=$(echo "$rss_values" | head -1)
    last_rss=$(echo "$rss_values" | tail -1)
    max_rss=$(echo "$rss_values" | sort -n | tail -1)

    if [ "$first_rss" -gt 0 ] 2>/dev/null; then
        growth_pct=$(( (last_rss - first_rss) * 100 / first_rss ))
    else
        growth_pct=0
    fi

    echo "Memory:"
    printf "  Initial: %d KB (%d MB)\n" "$first_rss" "$((first_rss / 1024))"
    printf "  Final:   %d KB (%d MB)\n" "$last_rss"  "$((last_rss / 1024))"
    printf "  Peak:    %d KB (%d MB)\n" "$max_rss"   "$((max_rss / 1024))"
    printf "  Growth:  %d%%\n" "$growth_pct"

    if [ "$growth_pct" -gt "$GROWTH_THRESHOLD" ]; then
        fail "Memory grew ${growth_pct}% (threshold: ${GROWTH_THRESHOLD}%) — possible leak"
        ((VERDICT_FAILURES++)) || true
    else
        pass "Memory growth ${growth_pct}% within ${GROWTH_THRESHOLD}% threshold"
    fi
else
    warn "No memory readings available"
fi
echo ""

# Error rate analysis
error_values=$(tail -n +2 "$TELEMETRY_CSV" | cut -d',' -f4)
if [ -n "$error_values" ]; then
    first_errors=$(echo "$error_values" | head -1)
    last_errors=$(echo "$error_values" | tail -1)
    echo "Error metric lines: first=${first_errors}, last=${last_errors}"

    if [ "$last_errors" -gt "$((first_errors + 10))" ] 2>/dev/null; then
        warn "Error metric lines increased from ${first_errors} to ${last_errors}"
    else
        pass "Error rate stable"
    fi
else
    warn "No error data collected"
fi
echo ""

# Pool count stability
pool_values=$(tail -n +2 "$TELEMETRY_CSV" | cut -d',' -f3 | grep -v '^0$' || true)
if [ -n "$pool_values" ]; then
    first_pools=$(echo "$pool_values" | head -1)
    last_pools=$(echo "$pool_values" | tail -1)
    echo "Pool count: first=${first_pools}, last=${last_pools}"

    diff_pools=$(( last_pools - first_pools ))
    abs_diff=${diff_pools#-}
    if [ "$abs_diff" -gt 10 ] 2>/dev/null; then
        warn "Pool count drifted by ${diff_pools} (first=${first_pools}, last=${last_pools})"
    else
        pass "Pool count stable (drift: ${diff_pools})"
    fi
else
    warn "No pool count data collected"
fi
echo ""

# pgbench aggregation
total_tps=0
total_committed=0
total_aborted=0
bench_files=0

for logfile in "$SOAK_DIR"/bench/tenant_*.log; do
    [ -f "$logfile" ] || continue
    ((bench_files++)) || true

    tps=$(awk '/tps = /{gsub(/.*tps = /,""); gsub(/[^0-9.].*/,""); print}' "$logfile" | tail -1)
    tps="${tps:-0}"
    committed=$(awk '/number of transactions actually processed:/{gsub(/.*: /,""); gsub(/[^0-9].*/,""); print}' "$logfile")
    committed="${committed:-0}"
    aborted=$(awk '/number of failed transactions:/{gsub(/.*: /,""); gsub(/[^0-9].*/,""); print}' "$logfile")
    aborted="${aborted:-0}"

    total_tps=$(echo "$total_tps + $tps" | bc 2>/dev/null || echo "$total_tps")
    total_committed=$((total_committed + committed))
    total_aborted=$((total_aborted + aborted))
done

# ── Time-series summary ──────────────────────────────────────────────────────

echo "--- Telemetry time-series (${TELEMETRY_CSV}) ---"
echo "  elapsed_s | rss_kb   | pools | errors"
echo "  ----------|----------|-------|-------"
tail -n +2 "$TELEMETRY_CSV" | while IFS=',' read -r t rss pools errs; do
    printf "  %9ss | %7s  | %5s | %s\n" "$t" "$rss" "$pools" "$errs"
done
echo ""

# ── Final verdict ────────────────────────────────────────────────────────────

echo "========================================"
echo "       Sustained Load Summary"
echo "========================================"
printf "  %-26s %s\n" "Duration:"               "${DURATION_MIN} min"
printf "  %-26s %s\n" "Tenants:"                "$bench_files"
printf "  %-26s %s\n" "Total TPS:"              "$total_tps"
printf "  %-26s %s\n" "Transactions committed:" "$total_committed"
printf "  %-26s %s\n" "Transactions aborted:"   "$total_aborted"
printf "  %-26s %s\n" "Telemetry samples:"      "$(tail -n +2 "$TELEMETRY_CSV" | wc -l | tr -d ' ')"
printf "  %-26s %s\n" "Results directory:"       "$SOAK_DIR"
echo "========================================"

if [ "$VERDICT_FAILURES" -gt 0 ]; then
    fail "Soak test: ${VERDICT_FAILURES} check(s) failed"
    exit 1
elif [ "$total_aborted" -gt 0 ]; then
    warn "Soak test passed with ${total_aborted} aborted transactions"
    exit 0
else
    pass "Soak test passed — no anomalies detected"
    exit 0
fi
