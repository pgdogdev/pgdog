#!/usr/bin/env bash
# Thin `apt-get install` wrapper that no-ops on non-Linux platforms (macOS
# developers running these scripts locally already have the deps installed
# via their own package manager).
#
# Usage: integration/ci/apt.sh <pkg1> <pkg2> ...
set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
    exit 0
fi

sudo apt-get install -y --no-install-recommends "$@"
