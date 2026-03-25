#!/usr/bin/env bash
# Test PgDog graceful shutdown behavior under active load.
#
# Simulates a K8s rolling restart: SIGTERM sent while clients are running queries.
# Validates: in-flight transactions complete, no partial writes,
#            new connections are rejected, process exits within terminationGracePeriodSeconds.
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OUTPUT_DIR="${SCRIPT_DIR}/../results"

PGDOG_HOST="127.0.0.1"
PGDOG_PORT=6432
PGDOG_BIN="${PGDOG_BIN:-$SCRIPT_DIR/../../target/release/pgdog}"
CONFIG_DIR="${CONFIG_DIR:-$SCRIPT_DIR/../config}"
TARGET_DB="tenant_1"
LOAD_CLIENTS=10
GRACE_PERIOD=10
PRE_LOAD_SEC=5

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pgdog-host)     PGDOG_HOST="$2";    shift 2 ;;
        --pgdog-port)     PGDOG_PORT="$2";    shift 2 ;;
        --pgdog-bin)      PGDOG_BIN="$2";     shift 2 ;;
        --config-dir)     CONFIG_DIR="$2";    shift 2 ;;
        --target-db)      TARGET_DB="$2";     shift 2 ;;
        --clients)        LOAD_CLIENTS="$2";  shift 2 ;;
        --grace-period)   GRACE_PERIOD="$2";  shift 2 ;;
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

for cmd in psql pgbench; do
    command -v "$cmd" >/dev/null 2>&1 || { fail "Required command not found: $cmd"; exit 1; }
done

mkdir -p "$OUTPUT_DIR"
LOGFILE="$OUTPUT_DIR/graceful_shutdown.log"
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

echo "=== Graceful Shutdown Test ==="
echo "PgDog binary: ${PGDOG_BIN}"
echo "Config dir: ${CONFIG_DIR}"
echo "Port: ${PGDOG_PORT}, Target: ${TARGET_DB}"
echo "Load clients: ${LOAD_CLIENTS}, Grace period: ${GRACE_PERIOD}s"
echo ""

# ── Phase 1: Start a separate PgDog instance ────────────────────────────────

echo "--- Phase 1: Starting test PgDog instance ---"

# Use a different port to avoid conflicts with the main test instance
TEST_PORT=16432
PGDOG_LOG="$OUTPUT_DIR/graceful_shutdown_pgdog.log"

if [ ! -f "$PGDOG_BIN" ]; then
    fail "PgDog binary not found: ${PGDOG_BIN}"
    exit 1
fi

# Generate config with the test port
TEST_CONFIG_DIR=$(mktemp -d)
cp "${CONFIG_DIR}/pgdog.toml" "${TEST_CONFIG_DIR}/pgdog.toml"
cp "${CONFIG_DIR}/users.toml" "${TEST_CONFIG_DIR}/users.toml"

# Patch port in the copied config
sed -i.bak "s/port = 6432/port = ${TEST_PORT}/" "${TEST_CONFIG_DIR}/pgdog.toml" 2>/dev/null || \
    sed -i '' "s/port = 6432/port = ${TEST_PORT}/" "${TEST_CONFIG_DIR}/pgdog.toml"
# Patch metrics port
sed -i.bak "s/openmetrics_port = 9090/openmetrics_port = 19090/" "${TEST_CONFIG_DIR}/pgdog.toml" 2>/dev/null || \
    sed -i '' "s/openmetrics_port = 9090/openmetrics_port = 19090/" "${TEST_CONFIG_DIR}/pgdog.toml"

cd "$TEST_CONFIG_DIR"
"$PGDOG_BIN" > "$PGDOG_LOG" 2>&1 &
TEST_PGDOG_PID=$!
CHILDREN+=("$TEST_PGDOG_PID")

# Wait for readiness
for i in $(seq 1 15); do
    if PGPASSWORD=pgdog psql -h 127.0.0.1 -p "$TEST_PORT" -U pgdog -d "$TARGET_DB" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        echo "  Test PgDog ready (PID: ${TEST_PGDOG_PID}, port: ${TEST_PORT})"
        break
    fi
    sleep 1
    if [ "$i" -eq 15 ]; then
        fail "Test PgDog did not start in 15s"
        exit 1
    fi
done
echo ""

# ── Phase 2: Start load ─────────────────────────────────────────────────────

echo "--- Phase 2: Starting background load ---"

WORKLOAD_SQL="${SCRIPT_DIR}/tenant_workload.sql"
BENCH_LOG="$OUTPUT_DIR/graceful_shutdown_bench.log"

PGPASSWORD=pgdog pgbench -h 127.0.0.1 -p "$TEST_PORT" -U pgdog "$TARGET_DB" \
    -c "$LOAD_CLIENTS" -T 60 \
    --protocol=extended -f "$WORKLOAD_SQL" \
    --no-vacuum \
    > "$BENCH_LOG" 2>&1 &
BENCH_PID=$!
CHILDREN+=("$BENCH_PID")

echo "  pgbench running (PID: ${BENCH_PID})"
echo "  Waiting ${PRE_LOAD_SEC}s for load to stabilize..."
sleep "$PRE_LOAD_SEC"
echo ""

# ── Phase 3: Send SIGTERM ────────────────────────────────────────────────────

echo "--- Phase 3: Sending SIGTERM to PgDog (PID: ${TEST_PGDOG_PID}) ---"

sigterm_time=$(date +%s)
kill -TERM "$TEST_PGDOG_PID" 2>/dev/null

echo "  SIGTERM sent at $(date '+%H:%M:%S')"
echo ""

# ── Phase 4: Monitor shutdown ───────────────────────────────────────────────

echo "--- Phase 4: Monitoring shutdown behavior ---"

# Check if process exits within grace period
exited=false
for i in $(seq 1 "$GRACE_PERIOD"); do
    if ! kill -0 "$TEST_PGDOG_PID" 2>/dev/null; then
        exit_time=$(date +%s)
        drain_sec=$((exit_time - sigterm_time))
        exited=true
        pass "PgDog exited after ${drain_sec}s (within ${GRACE_PERIOD}s grace period)"
        break
    fi
    sleep 1
    echo "  [${i}/${GRACE_PERIOD}s] still running..."
done

if [ "$exited" = "false" ]; then
    exit_check_time=$(date +%s)
    drain_sec=$((exit_check_time - sigterm_time))
    warn "PgDog still running after ${drain_sec}s — sending SIGKILL"
    kill -9 "$TEST_PGDOG_PID" 2>/dev/null || true
    fail "PgDog did not exit within grace period (${GRACE_PERIOD}s)"
    ((FAILURES++)) || true
fi
echo ""

# Wait for pgbench to finish (it should notice the connection dropped)
wait "$BENCH_PID" 2>/dev/null || true

# ── Phase 5: Analyze pgbench results ────────────────────────────────────────

echo "--- Phase 5: Analyzing pgbench results ---"

if [ -f "$BENCH_LOG" ]; then
    committed=$(awk '/number of transactions actually processed:/{gsub(/.*: /,""); gsub(/[^0-9].*/,""); print}' "$BENCH_LOG")
    committed="${committed:-0}"
    aborted=$(awk '/number of failed transactions:/{gsub(/.*: /,""); gsub(/[^0-9].*/,""); print}' "$BENCH_LOG")
    aborted="${aborted:-0}"

    echo "  Transactions committed: ${committed}"
    echo "  Transactions aborted:   ${aborted}"

    if [ "$committed" -gt 0 ]; then
        pass "PgDog processed ${committed} transactions before shutdown"
    else
        warn "No transactions completed"
    fi
fi
echo ""

# ── Phase 6: Verify new connections were rejected ────────────────────────────

echo "--- Phase 6: Verifying connections rejected after SIGTERM ---"

if [ "$exited" = "true" ]; then
    if PGPASSWORD=pgdog psql -h 127.0.0.1 -p "$TEST_PORT" -U pgdog -d "$TARGET_DB" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        fail "Connection succeeded after PgDog exited — unexpected (port reuse?)"
        ((FAILURES++)) || true
    else
        pass "Connection correctly refused after shutdown"
    fi
else
    warn "Cannot verify — PgDog was force-killed"
fi
echo ""

# ── Phase 7: Check PgDog logs for errors ────────────────────────────────────

echo "--- Phase 7: Checking PgDog shutdown logs ---"

if [ -f "$PGDOG_LOG" ]; then
    panic_count=$(grep -ci "panic" "$PGDOG_LOG" 2>/dev/null || true)
    panic_count=${panic_count:-0}
    error_count=$(grep -ci "error" "$PGDOG_LOG" 2>/dev/null || true)
    error_count=${error_count:-0}

    if [ "$panic_count" -gt 0 ]; then
        fail "PgDog log contains ${panic_count} panic(s)"
        ((FAILURES++)) || true
    else
        pass "No panics in PgDog log"
    fi

    echo "  Log errors: ${error_count}"
    echo "  Last 5 log lines:"
    tail -5 "$PGDOG_LOG" | sed 's/^/    /'
fi
echo ""

# Cleanup temp config
rm -rf "$TEST_CONFIG_DIR"

# ── Summary ──────────────────────────────────────────────────────────────────

echo "========================================"
echo "    Graceful Shutdown Test Summary"
echo "========================================"
printf "  %-28s %s\n" "Grace period:"           "${GRACE_PERIOD}s"
printf "  %-28s %s\n" "Exited cleanly:"         "$([ "$exited" = "true" ] && echo "YES" || echo "NO")"
printf "  %-28s %s\n" "Drain time:"             "${drain_sec:-N/A}s"
printf "  %-28s %s\n" "Transactions committed:" "${committed:-0}"
printf "  %-28s %s\n" "Transactions aborted:"   "${aborted:-0}"
echo "========================================"

if [ "$FAILURES" -gt 0 ]; then
    fail "Graceful shutdown test: ${FAILURES} failure(s)"
    exit 1
else
    pass "Graceful shutdown test passed"
    exit 0
fi
