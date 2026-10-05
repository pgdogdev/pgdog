#!/bin/bash
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Safety net: docker compose down and any stray pgdog processes are cleaned up
# on exit even if a scenario is interrupted mid-flight by timeout or signal.
cleanup() {
    (cd "${SCRIPT_DIR}" && docker compose down >/dev/null 2>&1 || true)
}
trap cleanup EXIT INT TERM

timeout --signal=TERM --kill-after=90s 16m bash "${SCRIPT_DIR}/reshard.sh"
timeout --signal=TERM --kill-after=90s 16m bash "${SCRIPT_DIR}/admin.sh"
timeout --signal=TERM --kill-after=90s 16m bash "${SCRIPT_DIR}/cli.sh"
