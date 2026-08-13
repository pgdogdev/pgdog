#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/../common.sh"

bash "${SCRIPT_DIR}/../ci/apt.sh" libpq-dev pkg-config

if ! command -v cabal >/dev/null 2>&1; then
    echo "cabal not found. Install GHC and Cabal, then re-run." >&2
    exit 1
fi

run_pgdog
wait_for_pgdog

bash ${SCRIPT_DIR}/dev.sh

stop_pgdog
