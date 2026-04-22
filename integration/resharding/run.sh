#!/bin/bash
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Safety net: docker compose down and any stray pgdog processes are cleaned up
# on exit even if dev.sh is interrupted mid-flight by timeout or signal.
cleanup() {
    (cd "${SCRIPT_DIR}" && docker compose down >/dev/null 2>&1 || true)
    killall -TERM pgdog 2>/dev/null || true
    sleep 1
    killall -KILL pgdog 2>/dev/null || true
}
trap cleanup EXIT INT TERM

timeout --signal=TERM --kill-after=90s 16m bash "${SCRIPT_DIR}/dev.sh"
