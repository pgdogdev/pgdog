#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_ROOT=$( cd -- "${SCRIPT_DIR}/.." &> /dev/null && pwd )
cd "${REPO_ROOT}"

SUITES=(
  "integration/pgbench/run.sh"
  "integration/go/run.sh"
  "integration/js/pg_tests/run.sh"
  "integration/python/run.sh"
  "integration/ruby/run.sh"
  "integration/java/run.sh"
  "integration/sql/run.sh"
  "integration/toxi/run.sh"
  "integration/rust/run.sh"
)

export PGDOG_KEEP_RUNNING=1

for suite in "${SUITES[@]}"; do
  echo "[ci-shared] Running ${suite}"
  bash "${suite}"
  echo "[ci-shared] Completed ${suite}"
done

unset PGDOG_KEEP_RUNNING
source "${SCRIPT_DIR}/common.sh"
stop_pgdog
