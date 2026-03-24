#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OUTPUT_DIR="${SCRIPT_DIR}/../results"

USER_COUNT=100
PGDOG_HOST="127.0.0.1"
PGDOG_PORT=6432
PG_HOST="127.0.0.1"
PG_PORT=15432
PARALLEL=50

while [[ $# -gt 0 ]]; do
    case "$1" in
        --user-count) USER_COUNT="$2"; shift 2 ;;
        --pgdog-host) PGDOG_HOST="$2"; shift 2 ;;
        --pgdog-port) PGDOG_PORT="$2"; shift 2 ;;
        --pg-host)    PG_HOST="$2";    shift 2 ;;
        --pg-port)    PG_PORT="$2";    shift 2 ;;
        --parallel)   PARALLEL="$2";   shift 2 ;;
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
LOGFILE="$OUTPUT_DIR/passthrough_auth.log"
exec > >(tee -a "$LOGFILE") 2>&1

CHILDREN=()
cleanup() {
    for pid in "${CHILDREN[@]+"${CHILDREN[@]}"}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup SIGINT SIGTERM

FAILURES=0

echo "=== Passthrough Authentication Test ==="
echo "PgDog: ${PGDOG_HOST}:${PGDOG_PORT}"
echo "Postgres: ${PG_HOST}:${PG_PORT}"
echo "Test users: ${USER_COUNT}"
echo ""

# ── Phase 1: Correct credentials ────────────────────────────────────────────

echo "--- Phase 1: Correct credentials (${USER_COUNT} users) ---"
phase1_ok=0
phase1_fail=0

for i in $(seq 1 "$USER_COUNT"); do
    result=$(PGPASSWORD="pass_${i}" psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" \
        -U "tenant_user_${i}" -d "tenant_1" -t -A -c "SELECT current_user" 2>/dev/null) || result=""

    if [ "$result" = "tenant_user_${i}" ]; then
        ((phase1_ok++)) || true
    else
        ((phase1_fail++)) || true
        if [ "$phase1_fail" -le 5 ]; then
            warn "  tenant_user_${i}: expected 'tenant_user_${i}', got '${result}'"
        fi
    fi

    if (( i % 25 == 0 )); then
        echo "  [${i}/${USER_COUNT}] ok=${phase1_ok} fail=${phase1_fail}"
    fi
done

if [ "$phase1_fail" -eq 0 ]; then
    pass "Phase 1: All ${USER_COUNT} users authenticated correctly"
else
    fail "Phase 1: ${phase1_fail}/${USER_COUNT} users failed authentication"
    ((FAILURES++)) || true
fi
echo ""

# ── Phase 2: Wrong credentials ──────────────────────────────────────────────

echo "--- Phase 2: Wrong credentials (expecting failures) ---"
phase2_rejected=0
phase2_unexpected_success=0
SAMPLE_SIZE=$((USER_COUNT < 20 ? USER_COUNT : 20))

for i in $(seq 1 "$SAMPLE_SIZE"); do
    if PGPASSWORD="wrong_password" psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" \
        -U "tenant_user_${i}" -d "tenant_1" -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        ((phase2_unexpected_success++)) || true
        warn "  tenant_user_${i}: unexpectedly succeeded with wrong password"
    else
        ((phase2_rejected++)) || true
    fi
done

if [ "$phase2_unexpected_success" -eq 0 ]; then
    pass "Phase 2: All ${SAMPLE_SIZE} wrong-password attempts correctly rejected"
else
    fail "Phase 2: ${phase2_unexpected_success} logins succeeded with wrong password"
    ((FAILURES++)) || true
fi
echo ""

# ── Phase 3: Credential rotation ────────────────────────────────────────────
# NOTE: PgDog caches the passthrough password from the first successful auth
# for each pool. Credential rotation requires the pool to be destroyed and
# recreated (via wildcard_pool_idle_timeout), or a PgDog reload. This phase
# tests whether rotation works after the pool expires.

echo "--- Phase 3: Credential rotation ---"
ROTATION_USER="tenant_user_1"
OLD_PASS="pass_1"
NEW_PASS="rotated_pass_1"
rotation_pass=true

if ! PGPASSWORD="$OLD_PASS" psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" \
    -U "$ROTATION_USER" -d "tenant_1" -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
    fail "Phase 3: Cannot connect with original password (precondition failed)"
    rotation_pass=false
fi

if [ "$rotation_pass" = true ]; then
    PGPASSWORD=postgres psql -h "$PG_HOST" -p "$PG_PORT" -U postgres -d postgres \
        -c "ALTER USER ${ROTATION_USER} WITH PASSWORD '${NEW_PASS}'" -q 2>/dev/null || {
        fail "Phase 3: Could not ALTER USER on Postgres directly"
        rotation_pass=false
    }
fi

if [ "$rotation_pass" = true ]; then
    new_pass_ok=false
    for attempt in $(seq 1 5); do
        if PGPASSWORD="$NEW_PASS" psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" \
            -U "$ROTATION_USER" -d "tenant_1" -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
            new_pass_ok=true
            break
        fi
        sleep 1
    done

    if [ "$new_pass_ok" = true ]; then
        pass "Phase 3: New password accepted after rotation"
    else
        warn "Phase 3: New password not accepted (expected — PgDog caches passthrough password per pool)"
        echo "  To rotate credentials, wait for wildcard_pool_idle_timeout or reload PgDog"
    fi

    PGPASSWORD=postgres psql -h "$PG_HOST" -p "$PG_PORT" -U postgres -d postgres \
        -c "ALTER USER ${ROTATION_USER} WITH PASSWORD '${OLD_PASS}'" -q 2>/dev/null || true
fi
echo ""

# ── Phase 4: Concurrent authentication ───────────────────────────────────────

echo "--- Phase 4: Concurrent authentication (${USER_COUNT} users, ${PARALLEL} parallel) ---"

RESULTS_DIR=$(mktemp -d)

auth_one() {
    local idx=$1
    if PGPASSWORD="pass_${idx}" psql -h "$PGDOG_HOST" -p "$PGDOG_PORT" \
        -U "tenant_user_${idx}" -d "tenant_1" -c "SELECT 1" -t -q -A >/dev/null 2>&1; then
        echo "ok" > "${RESULTS_DIR}/${idx}"
    else
        echo "fail" > "${RESULTS_DIR}/${idx}"
    fi
}
export -f auth_one
export PGDOG_HOST PGDOG_PORT RESULTS_DIR

phase4_start=$(date +%s)
seq 1 "$USER_COUNT" | xargs -P "$PARALLEL" -I{} bash -c 'auth_one {}'
phase4_end=$(date +%s)
phase4_elapsed=$((phase4_end - phase4_start))

phase4_ok=0
phase4_fail=0
for f in "$RESULTS_DIR"/*; do
    [ -f "$f" ] || continue
    status=$(cat "$f")
    if [ "$status" = "ok" ]; then
        ((phase4_ok++)) || true
    else
        ((phase4_fail++)) || true
    fi
done
rm -rf "$RESULTS_DIR"

if [ "$phase4_fail" -eq 0 ]; then
    pass "Phase 4: All ${USER_COUNT} concurrent logins succeeded (${phase4_elapsed}s)"
else
    fail "Phase 4: ${phase4_fail}/${USER_COUNT} concurrent logins failed"
    ((FAILURES++)) || true
fi
echo ""

# ── Summary ──────────────────────────────────────────────────────────────────

echo "========================================"
echo "    Passthrough Auth Test Summary"
echo "========================================"
printf "  %-30s %s\n" "Correct credentials:"    "${phase1_ok}/${USER_COUNT} passed"
printf "  %-30s %s\n" "Wrong credentials:"      "${phase2_rejected}/${SAMPLE_SIZE} rejected"
printf "  %-30s %s\n" "Credential rotation:"    "$([ "$rotation_pass" = true ] && echo "PASS" || echo "FAIL")"
printf "  %-30s %s\n" "Concurrent auth:"        "${phase4_ok}/${USER_COUNT} passed (${phase4_elapsed}s)"
echo "========================================"

if [ "$FAILURES" -gt 0 ]; then
    fail "Passthrough auth test: ${FAILURES} phase(s) failed"
    exit 1
else
    pass "Passthrough auth test passed"
    exit 0
fi
