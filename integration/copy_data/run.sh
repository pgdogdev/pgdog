#!/bin/bash
# Runs all copy_data integration tests in sequence.
#
# Tests:
#   data_sync/run.sh  — 0→2 and 2→2 resharding with live write traffic
#                       (uses local postgres from integration/setup.sh)
#   retry_test/run.sh — data-sync retry loop under mid-copy shard failure
#                       (manages its own docker-compose stack)
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "=== [copy_data] data_sync ==="
bash "${SCRIPT_DIR}/data_sync/run.sh"

echo "=== [copy_data] retry_test ==="
bash "${SCRIPT_DIR}/retry_test/run.sh"

echo "=== [copy_data] all tests passed ==="
