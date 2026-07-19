#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OUTPUT_DIR="${SCRIPT_DIR}/../results"

TENANT_COUNT=100
CLIENTS=10
DURATION=60
PROTOCOL="extended"
PGDOG_HOST="127.0.0.1"
PGDOG_PORT=6432

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tenant-count) TENANT_COUNT="$2"; shift 2 ;;
        --clients)      CLIENTS="$2";      shift 2 ;;
        --duration)     DURATION="$2";     shift 2 ;;
        --protocol)     PROTOCOL="$2";     shift 2 ;;
        --pgdog-host)   PGDOG_HOST="$2";   shift 2 ;;
        --pgdog-port)   PGDOG_PORT="$2";   shift 2 ;;
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

for cmd in psql pgbench curl shuf; do
    command -v "$cmd" >/dev/null 2>&1 || { fail "Required command not found: $cmd"; exit 1; }
done

WORKLOAD_SQL="${SCRIPT_DIR}/tenant_workload.sql"
if [ ! -f "$WORKLOAD_SQL" ]; then
    fail "Workload file not found: ${WORKLOAD_SQL}"
    exit 1
fi

BENCH_DIR="${OUTPUT_DIR}/bench_run_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BENCH_DIR"
LOGFILE="${BENCH_DIR}/multi_tenant_bench.log"
exec > >(tee -a "$LOGFILE") 2>&1

CHILDREN=()
cleanup() {
    for pid in "${CHILDREN[@]+"${CHILDREN[@]}"}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup SIGINT SIGTERM

# Distribute clients across tenants: at least 1 per tenant
clients_per_tenant=$((CLIENTS / TENANT_COUNT))
if [ "$clients_per_tenant" -lt 1 ]; then
    clients_per_tenant=1
fi
remainder=$((CLIENTS - clients_per_tenant * TENANT_COUNT))

echo "=== Multi-Tenant Benchmark ==="
echo "Tenants: ${TENANT_COUNT}, Total clients: ${CLIENTS}, Duration: ${DURATION}s"
echo "Protocol: ${PROTOCOL}, Clients/tenant: ${clients_per_tenant} (+${remainder} extra on first tenants)"
echo "Results: ${BENCH_DIR}"
echo ""

# Pick random tenant indices from the available pool
TENANT_IDS=$(shuf -i 1-"$TENANT_COUNT" -n "$TENANT_COUNT" | sort -n)

# ── Background metrics collector ─────────────────────────────────────────────

METRICS_DIR="${BENCH_DIR}/metrics"
mkdir -p "$METRICS_DIR"

collect_metrics() {
    local seq=0
    while true; do
        curl -sf "http://${PGDOG_HOST}:9090/metrics" > "${METRICS_DIR}/snapshot_$(printf '%04d' $seq).txt" 2>/dev/null || true
        ((seq++)) || true
        sleep 10
    done
}
collect_metrics &
METRICS_PID=$!
CHILDREN+=("$METRICS_PID")

# ── Launch pgbench workers ───────────────────────────────────────────────────

echo "--- Launching pgbench across ${TENANT_COUNT} tenants ---"
PIDS=()
idx=0

for tid in $TENANT_IDS; do
    c=$clients_per_tenant
    if [ "$remainder" -gt 0 ]; then
        c=$((c + 1))
        ((remainder--)) || true
    fi

    PGPASSWORD=pgdog pgbench -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog "tenant_$tid" \
        -c "$c" -T "$DURATION" \
        --protocol="$PROTOCOL" -f "$WORKLOAD_SQL" \
        --no-vacuum -P 5 \
        > "${BENCH_DIR}/bench_tenant_${tid}.log" 2>&1 &
    pid=$!
    PIDS+=("$pid")
    CHILDREN+=("$pid")

    ((idx++)) || true
    if (( idx % 20 == 0 )); then
        echo "  [${idx}/${TENANT_COUNT}] launched..."
    fi
done
echo "  [${idx}/${TENANT_COUNT}] all launched, waiting ${DURATION}s..."
echo ""

for pid in "${PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done

kill "$METRICS_PID" 2>/dev/null || true
wait "$METRICS_PID" 2>/dev/null || true

# ── Aggregate results ────────────────────────────────────────────────────────

echo "--- Aggregating results ---"

total_tps=0
total_committed=0
total_aborted=0
error_tenants=0
tenant_results=0
all_latencies=()

for logfile in "$BENCH_DIR"/bench_tenant_*.log; do
    [ -f "$logfile" ] || continue
    ((tenant_results++)) || true

    tps=$(awk '/tps = /{gsub(/.*tps = /,""); gsub(/[^0-9.].*/,""); print}' "$logfile" | tail -1)
    tps="${tps:-0}"
    committed=$(awk '/number of transactions actually processed:/{gsub(/.*: /,""); gsub(/[^0-9].*/,""); print}' "$logfile")
    committed="${committed:-0}"
    aborted=$(awk '/number of failed transactions:/{gsub(/.*: /,""); gsub(/[^0-9].*/,""); print}' "$logfile")
    aborted="${aborted:-0}"
    lat_avg=$(awk '/latency average =/{gsub(/.*= /,""); gsub(/[^0-9.].*/,""); print}' "$logfile")
    lat_avg="${lat_avg:-0}"

    total_tps=$(echo "$total_tps + $tps" | bc 2>/dev/null || echo "$total_tps")
    total_committed=$((total_committed + committed))
    total_aborted=$((total_aborted + aborted))

    if [ "$aborted" -gt 0 ]; then
        ((error_tenants++)) || true
    fi

    if [ "$lat_avg" != "0" ]; then
        all_latencies+=("$lat_avg")
    fi
done

# Compute latency percentiles from per-tenant averages
latency_p50="N/A"
latency_p95="N/A"
latency_p99="N/A"

if [ "${#all_latencies[@]}" -gt 0 ]; then
    sorted_lat=$(printf '%s\n' "${all_latencies[@]}" | sort -n)
    n=${#all_latencies[@]}
    idx_p50=$(( n * 50 / 100 ))
    idx_p95=$(( n * 95 / 100 ))
    idx_p99=$(( n * 99 / 100 ))
    # Clamp to valid range
    [ "$idx_p50" -ge "$n" ] && idx_p50=$((n - 1))
    [ "$idx_p95" -ge "$n" ] && idx_p95=$((n - 1))
    [ "$idx_p99" -ge "$n" ] && idx_p99=$((n - 1))

    latency_p50=$(echo "$sorted_lat" | sed -n "$((idx_p50 + 1))p")
    latency_p95=$(echo "$sorted_lat" | sed -n "$((idx_p95 + 1))p")
    latency_p99=$(echo "$sorted_lat" | sed -n "$((idx_p99 + 1))p")
fi

metrics_snapshots=$(find "$METRICS_DIR" -name "snapshot_*.txt" 2>/dev/null | wc -l | tr -d ' ')

echo ""
echo "========================================"
echo "    Multi-Tenant Benchmark Summary"
echo "========================================"
printf "  %-26s %s\n" "Tenants benchmarked:"    "$tenant_results"
printf "  %-26s %s\n" "Total TPS:"              "$total_tps"
printf "  %-26s %s\n" "Transactions committed:" "$total_committed"
printf "  %-26s %s\n" "Transactions aborted:"   "$total_aborted"
printf "  %-26s %s\n" "Tenants with errors:"    "$error_tenants"
printf "  %-26s %s ms\n" "Latency p50:"         "$latency_p50"
printf "  %-26s %s ms\n" "Latency p95:"         "$latency_p95"
printf "  %-26s %s ms\n" "Latency p99:"         "$latency_p99"
printf "  %-26s %s\n" "Metrics snapshots:"      "$metrics_snapshots"
printf "  %-26s %s\n" "Results directory:"       "$BENCH_DIR"
echo "========================================"

if [ "$total_aborted" -gt 0 ]; then
    warn "${total_aborted} transactions aborted across ${error_tenants} tenant(s)"
fi

if [ "$tenant_results" -eq "$TENANT_COUNT" ]; then
    pass "All ${TENANT_COUNT} tenants completed benchmark"
else
    fail "Only ${tenant_results}/${TENANT_COUNT} tenants produced results"
    exit 1
fi
