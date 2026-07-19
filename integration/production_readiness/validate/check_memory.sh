#!/bin/bash
set -euo pipefail

PGDOG_PID="${1:-}"
DURATION="${DURATION:-300}"
INTERVAL="${INTERVAL:-10}"
OUTPUT_DIR="${OUTPUT_DIR:-$(dirname "$0")/../results}"
GROWTH_THRESHOLD="${GROWTH_THRESHOLD:-20}"

mkdir -p "$OUTPUT_DIR"
MEMLOG="$OUTPUT_DIR/memory_trace.csv"

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

if [ -z "$PGDOG_PID" ]; then
    PGDOG_PID=$(pgrep -f "pgdog" | head -1 || true)
    if [ -z "$PGDOG_PID" ]; then
        fail "Cannot find PgDog process. Pass PID as first argument."
        exit 1
    fi
fi

if ! kill -0 "$PGDOG_PID" 2>/dev/null; then
    fail "PID $PGDOG_PID is not running"
    exit 1
fi

echo "=== Memory Monitor ==="
echo "PID: $PGDOG_PID"
echo "Duration: ${DURATION}s, Interval: ${INTERVAL}s"
echo "Growth threshold: ${GROWTH_THRESHOLD}%"
echo ""

echo "elapsed_s,rss_kb" > "$MEMLOG"

INITIAL_RSS=""
ELAPSED=0

while [ "$ELAPSED" -le "$DURATION" ]; do
    if ! kill -0 "$PGDOG_PID" 2>/dev/null; then
        warn "Process $PGDOG_PID exited at ${ELAPSED}s"
        break
    fi

    RSS=$(ps -o rss= -p "$PGDOG_PID" 2>/dev/null | tr -d ' ')
    if [ -z "$RSS" ]; then
        warn "Could not read RSS at ${ELAPSED}s"
        sleep "$INTERVAL"
        ELAPSED=$((ELAPSED + INTERVAL))
        continue
    fi

    echo "${ELAPSED},${RSS}" >> "$MEMLOG"

    if [ -z "$INITIAL_RSS" ]; then
        INITIAL_RSS="$RSS"
        echo "Initial RSS: ${RSS} KB ($(( RSS / 1024 )) MB)"
    fi

    RSS_MB=$(( RSS / 1024 ))
    printf "  [%4ds] RSS: %d KB (%d MB)\n" "$ELAPSED" "$RSS" "$RSS_MB"

    sleep "$INTERVAL"
    ELAPSED=$((ELAPSED + INTERVAL))
done

if [ -z "$INITIAL_RSS" ]; then
    fail "No memory readings collected"
    exit 1
fi

FINAL_RSS=$(tail -1 "$MEMLOG" | cut -d',' -f2)
GROWTH_PCT=$(( (FINAL_RSS - INITIAL_RSS) * 100 / INITIAL_RSS ))

echo ""
echo "=== Memory Summary ==="
echo "Initial: ${INITIAL_RSS} KB ($(( INITIAL_RSS / 1024 )) MB)"
echo "Final:   ${FINAL_RSS} KB ($(( FINAL_RSS / 1024 )) MB)"
echo "Growth:  ${GROWTH_PCT}%"
echo "Log:     $MEMLOG"
echo ""

if [ "$GROWTH_PCT" -gt "$GROWTH_THRESHOLD" ]; then
    fail "Memory grew ${GROWTH_PCT}% (threshold: ${GROWTH_THRESHOLD}%) — possible leak"
    exit 1
else
    pass "Memory growth ${GROWTH_PCT}% is within ${GROWTH_THRESHOLD}% threshold"
    exit 0
fi
