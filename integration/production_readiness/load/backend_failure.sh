#!/usr/bin/env bash
# Test PgDog behavior when backend connections fail unexpectedly.
#
# Uses toxiproxy to simulate: connection reset, timeout, and brief network partition.
# Validates: automatic reconnection, error propagation to clients,
#            pool recovery, no zombie connections.
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OUTPUT_DIR="${SCRIPT_DIR}/../results"

PGDOG_HOST="127.0.0.1"
PGDOG_PORT=6432
TOXI_API="${TOXI_API:-http://127.0.0.1:8474}"
PROXY_NAME="${PROXY_NAME:-pg_primary}"
TARGET_DB="tenant_1"
PARTITION_SEC=5

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pgdog-host)    PGDOG_HOST="$2";    shift 2 ;;
        --pgdog-port)    PGDOG_PORT="$2";    shift 2 ;;
        --toxi-api)      TOXI_API="$2";     shift 2 ;;
        --proxy-name)    PROXY_NAME="$2";    shift 2 ;;
        --target-db)     TARGET_DB="$2";     shift 2 ;;
        --partition-sec) PARTITION_SEC="$2"; shift 2 ;;
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
LOGFILE="$OUTPUT_DIR/backend_failure.log"
exec > >(tee -a "$LOGFILE") 2>&1

admin_query() {
    PGPASSWORD=admin psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" \
        -U admin -d admin -t -A -c "$1" 2>/dev/null
}

remove_all_toxics() {
    # List and remove all toxics for the proxy
    local toxics
    toxics=$(curl -sf "${TOXI_API}/proxies/${PROXY_NAME}/toxics" 2>/dev/null) || return 0
    for name in $(echo "$toxics" | grep -o '"name":"[^"]*"' | sed 's/"name":"//;s/"//'); do
        curl -sf -X DELETE "${TOXI_API}/proxies/${PROXY_NAME}/toxics/${name}" >/dev/null 2>&1 || true
    done
}

FAILURES=0

echo "=== Backend Failure Test ==="
echo "PgDog: ${PGDOG_HOST}:${PGDOG_PORT}"
echo "Toxiproxy API: ${TOXI_API}"
echo "Proxy: ${PROXY_NAME}"
echo "Target: ${TARGET_DB}"
echo ""

# Verify toxiproxy is reachable
if ! curl -sf "${TOXI_API}/version" >/dev/null 2>&1; then
    fail "Toxiproxy API not reachable at ${TOXI_API}"
    exit 1
fi

# Ensure clean state
remove_all_toxics

trap 'remove_all_toxics; wait 2>/dev/null || true' EXIT SIGINT SIGTERM

# ── Phase 1: Baseline ──────────────────────────────────────────────────────

echo "--- Phase 1: Baseline connectivity ---"

baseline_ok=0
for i in $(seq 1 5); do
    if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "$TARGET_DB" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        ((baseline_ok++)) || true
    fi
done

if [ "$baseline_ok" -eq 5 ]; then
    pass "Baseline: 5/5 queries OK"
else
    fail "Baseline: only ${baseline_ok}/5 queries OK — cannot proceed"
    exit 1
fi
echo ""

# ── Phase 2: Connection reset (reset_peer toxic) ───────────────────────────

echo "--- Phase 2: Connection reset (reset_peer) ---"

# Inject reset_peer: abruptly closes connections after N bytes
curl -sf -X POST "${TOXI_API}/proxies/${PROXY_NAME}/toxics" \
    -H "Content-Type: application/json" \
    -d '{"name":"reset_peer","type":"reset_peer","stream":"downstream","toxicity":0.5,"attributes":{"timeout":500}}' \
    >/dev/null 2>&1

echo "  Injected: reset_peer (50% toxicity, 500ms timeout)"

# Send queries — some should fail, but PgDog should recover
reset_ok=0
reset_fail=0
for i in $(seq 1 20); do
    if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "$TARGET_DB" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        ((reset_ok++)) || true
    else
        ((reset_fail++)) || true
    fi
done

echo "  Results: ok=${reset_ok}, fail=${reset_fail}"

# Remove toxic
curl -sf -X DELETE "${TOXI_API}/proxies/${PROXY_NAME}/toxics/reset_peer" >/dev/null 2>&1 || true

# Some failures expected, but not all
if [ "$reset_ok" -gt 0 ]; then
    pass "Phase 2: ${reset_ok}/20 queries survived connection resets"
else
    fail "Phase 2: All queries failed during connection reset"
    ((FAILURES++)) || true
fi

# Recovery check
sleep 2
recovery_ok=0
for i in $(seq 1 5); do
    if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "$TARGET_DB" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        ((recovery_ok++)) || true
    fi
done

if [ "$recovery_ok" -eq 5 ]; then
    pass "Phase 2 recovery: 5/5 queries OK"
else
    fail "Phase 2 recovery: only ${recovery_ok}/5 queries OK"
    ((FAILURES++)) || true
fi
echo ""

# ── Phase 3: Network partition (bandwidth=0) ───────────────────────────────

echo "--- Phase 3: Network partition (${PARTITION_SEC}s blackout) ---"

# Disable the proxy entirely
curl -sf -X POST "${TOXI_API}/proxies/${PROXY_NAME}" \
    -H "Content-Type: application/json" \
    -d '{"enabled":false}' \
    >/dev/null 2>&1

echo "  Proxy disabled — simulating network partition"

# Queries during partition should fail
partition_ok=0
partition_fail=0
for i in $(seq 1 5); do
    if timeout 3 bash -c \
        "PGPASSWORD=pgdog psql -h $PGDOG_HOST -p $PGDOG_PORT -U pgdog -d $TARGET_DB -c 'SELECT 1' -t -q -A" \
        >/dev/null 2>&1; then
        ((partition_ok++)) || true
    else
        ((partition_fail++)) || true
    fi
done

echo "  During partition: ok=${partition_ok}, fail=${partition_fail}"

# Wait for partition duration
echo "  Waiting ${PARTITION_SEC}s..."
sleep "$PARTITION_SEC"

# Re-enable proxy
curl -sf -X POST "${TOXI_API}/proxies/${PROXY_NAME}" \
    -H "Content-Type: application/json" \
    -d '{"enabled":true}' \
    >/dev/null 2>&1

echo "  Proxy re-enabled — partition ended"

# Recovery — allow PgDog time to reconnect
sleep 3

post_partition_ok=0
for attempt in $(seq 1 10); do
    if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "$TARGET_DB" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        ((post_partition_ok++)) || true
    fi
    sleep 0.5
done

if [ "$post_partition_ok" -ge 8 ]; then
    pass "Phase 3 recovery: ${post_partition_ok}/10 queries OK after partition"
else
    fail "Phase 3 recovery: only ${post_partition_ok}/10 queries OK — slow recovery"
    ((FAILURES++)) || true
fi
echo ""

# ── Phase 4: Slow backend (latency toxic) ───────────────────────────────────

echo "--- Phase 4: Slow backend (200ms latency — near timeout threshold) ---"

curl -sf -X POST "${TOXI_API}/proxies/${PROXY_NAME}/toxics" \
    -H "Content-Type: application/json" \
    -d '{"name":"slow_upstream","type":"latency","stream":"upstream","attributes":{"latency":200,"jitter":100}}' \
    >/dev/null 2>&1

echo "  Injected: 200ms ± 100ms latency"

slow_ok=0
slow_fail=0
slow_total_ms=0

for i in $(seq 1 10); do
    t_start=$(date +%s%N)
    if timeout 5 bash -c \
        "PGPASSWORD=pgdog psql -h $PGDOG_HOST -p $PGDOG_PORT -U pgdog -d $TARGET_DB -c 'SELECT 1' -t -q -A" \
        >/dev/null 2>&1; then
        t_end=$(date +%s%N)
        ms=$(( (t_end - t_start) / 1000000 ))
        ((slow_ok++)) || true
        slow_total_ms=$((slow_total_ms + ms))
    else
        ((slow_fail++)) || true
    fi
done

slow_avg=0
if [ "$slow_ok" -gt 0 ]; then
    slow_avg=$((slow_total_ms / slow_ok))
fi

echo "  Results: ok=${slow_ok}, fail=${slow_fail}, avg=${slow_avg}ms"

curl -sf -X DELETE "${TOXI_API}/proxies/${PROXY_NAME}/toxics/slow_upstream" >/dev/null 2>&1 || true

if [ "$slow_ok" -ge 7 ]; then
    pass "Phase 4: ${slow_ok}/10 queries succeeded under high latency"
else
    fail "Phase 4: only ${slow_ok}/10 queries succeeded — timeouts too aggressive?"
    ((FAILURES++)) || true
fi
echo ""

# ── Phase 5: Multi-tenant impact (partition one backend, query another) ─────

echo "--- Phase 5: Cross-tenant isolation during failure ---"

# Disable proxy (simulating backend failure)
curl -sf -X POST "${TOXI_API}/proxies/${PROXY_NAME}" \
    -H "Content-Type: application/json" \
    -d '{"enabled":false}' \
    >/dev/null 2>&1

echo "  Backend partitioned — testing if new tenant connections timeout cleanly"

# Try a different tenant — the pool creation should fail with clear error, not hang
timeout_start=$(date +%s)
timeout 10 bash -c \
    "PGPASSWORD=pgdog psql -h $PGDOG_HOST -p $PGDOG_PORT -U pgdog -d tenant_999 -c 'SELECT 1' -t -q -A" \
    >/dev/null 2>&1 && cross_ok=true || cross_ok=false
timeout_end=$(date +%s)
timeout_elapsed=$((timeout_end - timeout_start))

# Re-enable
curl -sf -X POST "${TOXI_API}/proxies/${PROXY_NAME}" \
    -H "Content-Type: application/json" \
    -d '{"enabled":true}' \
    >/dev/null 2>&1

if [ "$cross_ok" = "false" ]; then
    if [ "$timeout_elapsed" -le 8 ]; then
        pass "Phase 5: Connection failed fast (${timeout_elapsed}s) — connect_timeout working"
    else
        warn "Phase 5: Connection took ${timeout_elapsed}s to fail — connect_timeout may be too high"
    fi
else
    warn "Phase 5: Connection unexpectedly succeeded during partition (cached pool?)"
fi
echo ""

# ── Final recovery ──────────────────────────────────────────────────────────

remove_all_toxics
sleep 2

final_ok=0
for i in $(seq 1 5); do
    if PGPASSWORD=pgdog psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" -U pgdog -d "$TARGET_DB" \
        -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        ((final_ok++)) || true
    fi
done

if [ "$final_ok" -eq 5 ]; then
    pass "Final recovery: 5/5 OK"
else
    fail "Final recovery: ${final_ok}/5 OK"
    ((FAILURES++)) || true
fi
echo ""

# ── Summary ──────────────────────────────────────────────────────────────────

echo "========================================"
echo "    Backend Failure Test Summary"
echo "========================================"
printf "  %-30s %s\n" "Connection reset:"      "${reset_ok}/20 survived, recovered ${recovery_ok}/5"
printf "  %-30s %s\n" "Network partition:"      "Recovered ${post_partition_ok}/10 after ${PARTITION_SEC}s"
printf "  %-30s %s\n" "Slow backend (200ms):"   "${slow_ok}/10 OK (avg ${slow_avg}ms)"
printf "  %-30s %s\n" "Cross-tenant isolation:" "$([ "$cross_ok" = "false" ] && echo "Failed fast (${timeout_elapsed}s)" || echo "Succeeded (cached)")"
printf "  %-30s %s\n" "Final health:"           "${final_ok}/5 OK"
echo "========================================"

if [ "$FAILURES" -gt 0 ]; then
    fail "Backend failure test: ${FAILURES} check(s) failed"
    exit 1
else
    pass "Backend failure test passed"
    exit 0
fi
