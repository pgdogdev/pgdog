#!/usr/bin/env bash
# Thin `apt-get install` wrapper used in CI only.
# No-ops when CI is not set (local dev) or apt-get is unavailable.
#
# Usage: integration/ci/apt.sh <pkg1> <pkg2> ...
set -euo pipefail

if [[ -z "${CI:-}" ]] || ! command -v apt-get &>/dev/null; then
    exit 0
fi

sudo apt-get install -y --no-install-recommends "$@"
