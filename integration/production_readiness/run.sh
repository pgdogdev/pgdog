#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

TENANT_COUNT="${TENANT_COUNT:-2000}"
DURATION="${DURATION:-60}"
POOL_SIZE="${POOL_SIZE:-1}"
MAX_WILDCARD_POOLS="${MAX_WILDCARD_POOLS:-0}"
WILDCARD_IDLE_TIMEOUT="${WILDCARD_IDLE_TIMEOUT:-15}"
PGDOG_PORT="${PGDOG_PORT:-6432}"
PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-15432}"
SKIP_BUILD="${SKIP_BUILD:-false}"
SKIP_INFRA="${SKIP_INFRA:-false}"
SKIP_K8S="${SKIP_K8S:-true}"
RESULTS_DIR="${RESULTS_DIR:-${SCRIPT_DIR}/results}"
CLIENTS="${CLIENTS:-50}"
LOG_LEVEL="${LOG_LEVEL:-off}"
LOAD_TENANTS="${LOAD_TENANTS:-100}"
AUTH_USERS="${AUTH_USERS:-100}"
LIFECYCLE_CYCLES="${LIFECYCLE_CYCLES:-3}"
LIFECYCLE_BATCH="${LIFECYCLE_BATCH:-50}"
STORM_SIZE="${STORM_SIZE:-200}"
STORM_PARALLEL="${STORM_PARALLEL:-100}"
INJECT_LATENCY="${INJECT_LATENCY:-5}"
INJECT_JITTER="${INJECT_JITTER:-2}"
SCALE_MIN_SUCCESS_RATE="${SCALE_MIN_SUCCESS_RATE:-85}"

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
        --wildcard-idle-timeout) WILDCARD_IDLE_TIMEOUT="$2"; shift 2 ;;
        --clients)            CLIENTS="$2"; shift 2 ;;
        --pg-host)            PG_HOST="$2"; shift 2 ;;
        --pg-port)            PG_PORT="$2"; shift 2 ;;
        --pgdog-port)         PGDOG_PORT="$2"; shift 2 ;;
        --log-level)          LOG_LEVEL="$2"; shift 2 ;;
        --results-dir)        RESULTS_DIR="$2"; shift 2 ;;
        --load-tenants)       LOAD_TENANTS="$2"; shift 2 ;;
        --auth-users)         AUTH_USERS="$2"; shift 2 ;;
        --lifecycle-cycles)   LIFECYCLE_CYCLES="$2"; shift 2 ;;
        --lifecycle-batch)    LIFECYCLE_BATCH="$2"; shift 2 ;;
        --storm-size)         STORM_SIZE="$2"; shift 2 ;;
        --storm-parallel)     STORM_PARALLEL="$2"; shift 2 ;;
        --inject-latency)     INJECT_LATENCY="$2"; shift 2 ;;
        --inject-jitter)      INJECT_JITTER="$2"; shift 2 ;;
        --scale-min-success-rate) SCALE_MIN_SUCCESS_RATE="$2"; shift 2 ;;
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
            echo "  --max-wildcard-pools N  Max wildcard pools, 0=unlimited (default: 0)"
            echo "  --wildcard-idle-timeout N  Idle seconds before pool eviction (default: 15)"
            echo "  --clients N             Number of clients (default: 50)"
            echo "  --pg-host HOST          Postgres host (default: 127.0.0.1)"
            echo "  --pg-port PORT          Postgres port (default: 15432)"
            echo "  --pgdog-port PORT       PgDog listen port (default: 6432)"
            echo "  --log-level LEVEL       PgDog log level: off|error|warn|info|debug|trace (default: off)"
            echo "  --results-dir DIR       Directory for test results (default: ./results)"
            echo "  --load-tenants N        Tenants for load/sustained tests (default: 100)"
            echo "  --auth-users N          Users for passthrough auth test (default: 100)"
            echo "  --lifecycle-cycles N    Pool lifecycle churn cycles (default: 3)"
            echo "  --lifecycle-batch N     Pool lifecycle batch size (default: 50)"
            echo "  --storm-size N          Connection storm total connections (default: 200)"
            echo "  --storm-parallel N      Connection storm parallelism (default: 100)"
            echo "  --inject-latency N      Toxiproxy injected latency in ms (default: 5)"
            echo "  --inject-jitter N       Toxiproxy injected jitter in ms (default: 2)"
            echo "  --scale-min-success-rate N  Minimum acceptable scale-connect success %% (default: 85)"
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
echo "  Wildcard idle timeout: ${WILDCARD_IDLE_TIMEOUT}s"
echo "  PgDog port:         $PGDOG_PORT"
echo "  Postgres:           $PG_HOST:$PG_PORT"
echo "  Log level:          $LOG_LEVEL"
echo "  Results dir:        $RESULTS_DIR"
echo "  Load tenants:       $LOAD_TENANTS"
echo "  Auth users:         $AUTH_USERS"
echo "  Lifecycle cycles:   $LIFECYCLE_CYCLES (batch: $LIFECYCLE_BATCH)"
echo "  Storm size:         $STORM_SIZE (parallel: $STORM_PARALLEL)"
echo "  Inject latency:     ${INJECT_LATENCY}ms (jitter: ${INJECT_JITTER}ms)"
echo "  Scale min success:  ${SCALE_MIN_SUCCESS_RATE}%"
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
    QUIET=true bash "$SCRIPT_DIR/setup/generate_tenants.sh" \
        --count "$TENANT_COUNT" --host "$PG_HOST" --port "$PG_PORT"

    echo "Configuring toxiproxy..."
    bash "$SCRIPT_DIR/setup/configure_toxiproxy.sh" || {
        echo -e "${YELLOW}Toxiproxy setup failed (fault injection tests will be skipped)${NC}"
    }
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
    --wildcard-idle-timeout "$WILDCARD_IDLE_TIMEOUT" \
    --host "$PG_HOST" \
    --port "$PG_PORT" \
    --output-dir "$SCRIPT_DIR/config"
echo ""

# ── Phase 4: Start PgDog ────────────────────────────────────────────
echo -e "${CYAN}═══ Phase 4: Starting PgDog ═══${NC}"
cd "$SCRIPT_DIR/config"
RUST_LOG="$LOG_LEVEL" "$PGDOG_BIN" &
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
        --count "$TENANT_COUNT" --pgdog-port "$PGDOG_PORT" \
        --min-success-rate "$SCALE_MIN_SUCCESS_RATE"
echo ""

# Let idle connections drain before next test (idle_timeout=10s)
echo "Waiting 15s for idle backend connections to drain..."
sleep 15

# 5b: Multi-tenant load test
echo -e "${BOLD}── Multi-Tenant Load Test ──${NC}"
run_test "Multi-tenant load ($LOAD_TENANTS tenants, ${CLIENTS} clients, ${DURATION}s)" "$RESULTS_DIR/multi_tenant_bench.log" \
    bash "$SCRIPT_DIR/load/multi_tenant_bench.sh" \
        --tenant-count "$LOAD_TENANTS" --clients "$CLIENTS" --duration "$DURATION" \
        --pgdog-port "$PGDOG_PORT"
echo ""

# Let connections drain after heavy load before auth test
echo "Waiting 20s for backend connections to drain after load test..."
sleep 20

# 5c: Passthrough auth
echo -e "${BOLD}── Passthrough Auth ──${NC}"
run_test "Passthrough auth ($AUTH_USERS users)" "$RESULTS_DIR/passthrough_auth.log" \
    bash "$SCRIPT_DIR/load/passthrough_auth.sh" \
        --user-count "$AUTH_USERS" --pgdog-port "$PGDOG_PORT" --pg-port "$PG_PORT"
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
        --duration "$DURATION_MINS" --clients "$CLIENTS" --tenants "$LOAD_TENANTS" \
        --pgdog-port "$PGDOG_PORT" --pgdog-pid "$PGDOG_PID"
echo ""

# Let pools drain before lifecycle test
echo "Waiting 15s for pools to drain..."
sleep 15

# 5g: Pool lifecycle churn
echo -e "${BOLD}── Pool Lifecycle Churn ──${NC}"
run_test "Pool lifecycle churn ($LIFECYCLE_CYCLES cycles)" "$RESULTS_DIR/pool_lifecycle.log" \
    bash "$SCRIPT_DIR/load/pool_lifecycle.sh" \
        --pgdog-port "$PGDOG_PORT" --idle-timeout "$WILDCARD_IDLE_TIMEOUT" --cycles "$LIFECYCLE_CYCLES" --batch-size "$LIFECYCLE_BATCH"
echo ""

# 5h: Connection storm (thundering herd)
# Target range must stay within existing tenants
STORM_RANGE_END=$((TENANT_COUNT < 700 ? TENANT_COUNT : 700))
STORM_RANGE_START=$(( STORM_RANGE_END > 200 ? STORM_RANGE_END - 200 : 1 ))
echo -e "${BOLD}── Connection Storm ──${NC}"
run_test "Connection storm ($STORM_SIZE concurrent to cold pools)" "$RESULTS_DIR/connection_storm.log" \
    bash "$SCRIPT_DIR/load/connection_storm.sh" \
        --pgdog-port "$PGDOG_PORT" --storm-size "$STORM_SIZE" --parallel "$STORM_PARALLEL" \
        --range-start "$STORM_RANGE_START" --range-end "$STORM_RANGE_END"
echo ""

# 5i: Idle-in-transaction blocking
echo -e "${BOLD}── Idle-in-Transaction ──${NC}"
run_test "Idle-in-transaction (pool starvation)" "$RESULTS_DIR/idle_in_transaction.log" \
    bash "$SCRIPT_DIR/load/idle_in_transaction.sh" \
        --pgdog-port "$PGDOG_PORT" --target-db "tenant_1"
echo ""

# 5j: Backend failure (requires toxiproxy)
if curl -sf "http://127.0.0.1:8474/version" >/dev/null 2>&1; then
    echo -e "${BOLD}── Backend Failure (toxiproxy) ──${NC}"
    run_test "Backend failure (reset, partition, slow)" "$RESULTS_DIR/backend_failure.log" \
        bash "$SCRIPT_DIR/load/backend_failure.sh" \
            --pgdog-port "$PGDOG_PORT" --target-db "tenant_1"
    echo ""

    echo -e "${BOLD}── Network Latency (toxiproxy) ──${NC}"
    COLD_RANGE_START=$(( LOAD_TENANTS + 1 ))
    if [ "$COLD_RANGE_START" -gt "$TENANT_COUNT" ]; then
        COLD_RANGE_START=1
    fi
    COLD_RANGE_END=$(( COLD_RANGE_START + 49 ))
    if [ "$COLD_RANGE_END" -gt "$TENANT_COUNT" ]; then
        COLD_RANGE_END="$TENANT_COUNT"
    fi
    run_test "Network latency (${INJECT_LATENCY}ms injected)" "$RESULTS_DIR/network_latency.log" \
        bash "$SCRIPT_DIR/load/network_latency.sh" \
            --pgdog-port "$PGDOG_PORT" --latency "$INJECT_LATENCY" --jitter "$INJECT_JITTER" \
            --cold-range-start "$COLD_RANGE_START" --cold-range-end "$COLD_RANGE_END"
    echo ""
else
    echo -e "${YELLOW}Skipping toxiproxy tests (toxiproxy not available)${NC}"
    echo ""
fi

# 5k: Graceful shutdown (runs last — kills PgDog)
echo -e "${BOLD}── Graceful Shutdown ──${NC}"
run_test "Graceful shutdown under load" "$RESULTS_DIR/graceful_shutdown.log" \
    bash "$SCRIPT_DIR/load/graceful_shutdown.sh" \
        --pgdog-port "$PGDOG_PORT" --pgdog-bin "$PGDOG_BIN" --config-dir "$SCRIPT_DIR/config"
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
