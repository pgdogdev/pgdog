#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

TENANT_COUNT="${TENANT_COUNT:-2000}"
DURATION="${DURATION:-60}"
POOL_SIZE="${POOL_SIZE:-1}"
MAX_WILDCARD_POOLS="${MAX_WILDCARD_POOLS:-400}"
PGDOG_PORT="${PGDOG_PORT:-6432}"
PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-15432}"
SKIP_BUILD="${SKIP_BUILD:-false}"
SKIP_INFRA="${SKIP_INFRA:-false}"
SKIP_K8S="${SKIP_K8S:-true}"
RESULTS_DIR="${SCRIPT_DIR}/results"
CLIENTS="${CLIENTS:-50}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

if ! [ -t 1 ]; then
    RED='' GREEN='' YELLOW='' CYAN='' BOLD='' NC=''
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tenant-count)       TENANT_COUNT="$2"; shift 2 ;;
        --duration)           DURATION="$2"; shift 2 ;;
        --pool-size)          POOL_SIZE="$2"; shift 2 ;;
        --max-wildcard-pools) MAX_WILDCARD_POOLS="$2"; shift 2 ;;
        --clients)            CLIENTS="$2"; shift 2 ;;
        --skip-build)         SKIP_BUILD=true; shift ;;
        --skip-infra)         SKIP_INFRA=true; shift ;;
        --skip-k8s)           SKIP_K8S=true; shift ;;
        --with-k8s)           SKIP_K8S=false; shift ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --tenant-count N        Number of tenant databases (default: 2000)"
            echo "  --duration N            Load test duration in seconds (default: 60)"
            echo "  --pool-size N           Default pool size (default: 1)"
            echo "  --max-wildcard-pools N  Max wildcard pools, 0=unlimited (default: 400)"
            echo "  --clients N             Number of clients (default: 50)"
            echo "  --skip-build            Skip cargo build"
            echo "  --skip-infra            Skip Docker Compose and tenant setup"
            echo "  --skip-k8s              Skip K8s tests (default)"
            echo "  --with-k8s              Include K8s tests"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

mkdir -p "$RESULTS_DIR"
SUMMARY_FILE="$RESULTS_DIR/summary.txt"
: > "$SUMMARY_FILE"

TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
PGDOG_PID=""

record_result() {
    local name="$1" status="$2"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if [ "$status" -eq 0 ]; then
        PASSED_TESTS=$((PASSED_TESTS + 1))
        echo -e "${GREEN}[PASS]${NC} $name"
        echo "PASS: $name" >> "$SUMMARY_FILE"
    else
        FAILED_TESTS=$((FAILED_TESTS + 1))
        echo -e "${RED}[FAIL]${NC} $name"
        echo "FAIL: $name" >> "$SUMMARY_FILE"
    fi
}

cleanup() {
    echo ""
    echo -e "${CYAN}Cleaning up...${NC}"
    if [ -n "$PGDOG_PID" ] && kill -0 "$PGDOG_PID" 2>/dev/null; then
        kill "$PGDOG_PID" 2>/dev/null || true
        wait "$PGDOG_PID" 2>/dev/null || true
    fi
    if [ "$SKIP_INFRA" = "false" ]; then
        docker compose -f "$SCRIPT_DIR/docker-compose.yml" down -v 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

echo -e "${BOLD}╔══════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║   PgDog Production Readiness Test Suite      ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════╝${NC}"
echo ""
echo "Configuration:"
echo "  Tenant count:       $TENANT_COUNT"
echo "  Load duration:      ${DURATION}s"
echo "  Pool size:          $POOL_SIZE"
echo "  Max wildcard pools: $MAX_WILDCARD_POOLS"
echo "  PgDog port:         $PGDOG_PORT"
echo "  Postgres:           $PG_HOST:$PG_PORT"
echo ""

# ── Phase 1: Build ──────────────────────────────────────────────────
if [ "$SKIP_BUILD" = "false" ]; then
    echo -e "${CYAN}═══ Phase 1: Building PgDog ═══${NC}"
    cd "$SCRIPT_DIR/../.."
    cargo build --release 2>&1 | tail -5
    PGDOG_BIN="$SCRIPT_DIR/../../target/release/pgdog"
    echo "Binary: $PGDOG_BIN"
    echo ""
else
    PGDOG_BIN="${PGDOG_BIN:-$SCRIPT_DIR/../../target/release/pgdog}"
    echo -e "${YELLOW}Skipping build (using $PGDOG_BIN)${NC}"
    echo ""
fi

if [ ! -f "$PGDOG_BIN" ]; then
    echo -e "${RED}PgDog binary not found at $PGDOG_BIN${NC}"
    echo "Run without --skip-build or set PGDOG_BIN"
    exit 1
fi

# ── Phase 2: Infrastructure ─────────────────────────────────────────
if [ "$SKIP_INFRA" = "false" ]; then
    echo -e "${CYAN}═══ Phase 2: Starting Infrastructure ═══${NC}"
    cd "$SCRIPT_DIR"
    docker compose -f docker-compose.yml up -d
    echo "Waiting for Postgres to be ready..."
    for i in $(seq 1 30); do
        if PGPASSWORD=postgres psql -h "$PG_HOST" -p "$PG_PORT" -U postgres -c "SELECT 1" -t -q 2>/dev/null | grep -q 1; then
            echo "Postgres ready after ${i}s"
            break
        fi
        sleep 1
        if [ "$i" -eq 30 ]; then
            echo -e "${RED}Postgres did not become ready in 30s${NC}"
            exit 1
        fi
    done

    echo "Creating $TENANT_COUNT tenant databases..."
    bash "$SCRIPT_DIR/setup/generate_tenants.sh" \
        --count "$TENANT_COUNT" --host "$PG_HOST" --port "$PG_PORT"
    echo ""
else
    echo -e "${YELLOW}Skipping infrastructure setup${NC}"
    echo ""
fi

# ── Phase 3: Generate PgDog Config ──────────────────────────────────
echo -e "${CYAN}═══ Phase 3: Generating PgDog Config ═══${NC}"
python3 "$SCRIPT_DIR/setup/generate_config.py" \
    --pool-size "$POOL_SIZE" \
    --max-wildcard-pools "$MAX_WILDCARD_POOLS" \
    --host "$PG_HOST" \
    --port "$PG_PORT" \
    --output-dir "$SCRIPT_DIR/config"
echo ""

# ── Phase 4: Start PgDog ────────────────────────────────────────────
echo -e "${CYAN}═══ Phase 4: Starting PgDog ═══${NC}"
cd "$SCRIPT_DIR/config"
"$PGDOG_BIN" &
PGDOG_PID=$!
echo "PgDog PID: $PGDOG_PID"

for i in $(seq 1 15); do
    if PGPASSWORD=pgdog psql -h 127.0.0.1 -p "$PGDOG_PORT" -U pgdog -d "tenant_1" -c "SELECT 1" -t -q 2>/dev/null | grep -q 1; then
        echo "PgDog ready after ${i}s"
        break
    fi
    sleep 1
    if [ "$i" -eq 15 ]; then
        echo -e "${RED}PgDog did not start in 15s${NC}"
        exit 1
    fi
done
echo ""

# ── Phase 5: Run Tests ──────────────────────────────────────────────
echo -e "${CYAN}═══ Phase 5: Running Tests ═══${NC}"
echo ""

# Helper: run a test script, capture exit code without aborting the runner.
run_test() {
    local name="$1"; shift
    local logfile="$1"; shift
    set +e
    "$@" 2>&1 | tee "$logfile"
    local rc=${PIPESTATUS[0]}
    set -e
    record_result "$name" "$rc"
}

# 5a: Scale connection test
echo -e "${BOLD}── Scale Connect (${TENANT_COUNT} databases) ──${NC}"
run_test "Scale connect ($TENANT_COUNT DBs)" "$RESULTS_DIR/scale_connect.log" \
    bash "$SCRIPT_DIR/load/scale_connect.sh" \
        --count "$TENANT_COUNT" --pgdog-port "$PGDOG_PORT"
echo ""

# Let idle connections drain before next test (idle_timeout=10s)
echo "Waiting 15s for idle backend connections to drain..."
sleep 15

# 5b: Multi-tenant load test
echo -e "${BOLD}── Multi-Tenant Load Test ──${NC}"
run_test "Multi-tenant load (100 tenants, ${CLIENTS} clients, ${DURATION}s)" "$RESULTS_DIR/multi_tenant_bench.log" \
    bash "$SCRIPT_DIR/load/multi_tenant_bench.sh" \
        --tenant-count 100 --clients "$CLIENTS" --duration "$DURATION" \
        --pgdog-port "$PGDOG_PORT"
echo ""

# Let connections drain after heavy load before auth test
echo "Waiting 20s for backend connections to drain after load test..."
sleep 20

# 5c: Passthrough auth
echo -e "${BOLD}── Passthrough Auth ──${NC}"
run_test "Passthrough auth (100 users)" "$RESULTS_DIR/passthrough_auth.log" \
    bash "$SCRIPT_DIR/load/passthrough_auth.sh" \
        --user-count 100 --pgdog-port "$PGDOG_PORT" --pg-port "$PG_PORT"
echo ""

# 5d: Pool pressure
echo -e "${BOLD}── Pool Pressure ──${NC}"
run_test "Pool pressure (pool=$POOL_SIZE, conns=$CLIENTS)" "$RESULTS_DIR/pool_pressure.log" \
    bash "$SCRIPT_DIR/load/pool_pressure.sh" \
        --pool-size "$POOL_SIZE" --connections "$CLIENTS" --pgdog-port "$PGDOG_PORT"
echo ""

# 5e: Observability
echo -e "${BOLD}── Observability Validation ──${NC}"
run_test "OpenMetrics validation" "$RESULTS_DIR/check_metrics.log" \
    bash "$SCRIPT_DIR/validate/check_metrics.sh"

run_test "Admin pool validation" "$RESULTS_DIR/check_pools.log" \
    bash "$SCRIPT_DIR/validate/check_pools.sh"
echo ""

# 5f: Sustained load (shorter for automated run)
echo -e "${BOLD}── Sustained Load (soak) ──${NC}"
DURATION_MINS=$(( (DURATION + 59) / 60 ))
run_test "Sustained load (${DURATION_MINS}m, memory check)" "$RESULTS_DIR/sustained_load.log" \
    bash "$SCRIPT_DIR/load/sustained_load.sh" \
        --duration "$DURATION_MINS" --clients "$CLIENTS" --tenants 100 \
        --pgdog-port "$PGDOG_PORT" --pgdog-pid "$PGDOG_PID"
echo ""

# ── Phase 6: Summary ────────────────────────────────────────────────
echo -e "${BOLD}╔══════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║   Test Results Summary                       ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════╝${NC}"
echo ""
echo "  Total:  $TOTAL_TESTS"
echo -e "  Passed: ${GREEN}$PASSED_TESTS${NC}"
echo -e "  Failed: ${RED}$FAILED_TESTS${NC}"
echo ""
echo "  Results: $RESULTS_DIR/"
echo "  Summary: $SUMMARY_FILE"
echo ""

cat "$SUMMARY_FILE"
echo ""

if [ "$FAILED_TESTS" -gt 0 ]; then
    echo -e "${RED}${BOLD}VERDICT: NOT PRODUCTION READY (${FAILED_TESTS} failure(s))${NC}"
    exit 1
else
    echo -e "${GREEN}${BOLD}VERDICT: ALL TESTS PASSED${NC}"
    exit 0
fi
