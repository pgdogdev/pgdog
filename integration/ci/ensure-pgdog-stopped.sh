#!/usr/bin/env bash
# Make sure no pgdog process is left behind — coverage profile collection
# needs the process to exit cleanly.
set -euo pipefail

if pgrep -x pgdog > /dev/null; then
    killall -TERM pgdog
    sleep 5
fi
