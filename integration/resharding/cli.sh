#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/shared.sh"

CLI_SLOT=resharding_cli

initialize_resharding
pgdog_cli schema-sync
pgdog_cli data-sync --sync-only --skip-schema-sync --replication-slot "${CLI_SLOT}"
pgdog_cli schema-sync --phase post
"${PGDOG_BIN}" "${PGDOG_ARGS[@]}" data-sync "${SYNC_ARGS[@]}" \
    --replicate-only --skip-schema-sync --replication-slot "${CLI_SLOT}" &
MIGRATION_PID=$!
prepare_resharding_verification
wait_for_replication_catchup
pgdog_cli schema-sync --validation
verify_resharding_data
