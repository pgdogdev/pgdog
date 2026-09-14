#!/usr/bin/env bash
# hyperfine --prepare: put the destination shards into the state the timed
# stage expects.
#
#   pre_data   destination is empty
#   post_data  pre-data schema is applied
#   cutover    pre-data and post-data schema are applied
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

STAGE="${1:?usage: prepare.sh <pre_data|post_data|cutover>}"

# PGDOG_BIN and PGDOG_CONFIG are exported by run.sh; this script is not standalone.
PGDOG_CONFIG="${PGDOG_CONFIG:-${SETUP_DIR}/pgdog.toml}"

export PGHOST=127.0.0.1
export PGPORT=5432
export PGUSER=pgdog
export PGPASSWORD=pgdog
export PGOPTIONS="-c client_min_messages=warning"

DEST_DBS=(shard_0 shard_1 shard_2 shard_3)

schema_sync() {
    "${PGDOG_BIN}" \
        --config "${PGDOG_CONFIG}" \
        --users "${SETUP_DIR}/users.toml" \
        schema-sync \
        --from-database source \
        --to-database destination \
        --publication bench_schema \
        "$@" >/dev/null
}

echo ">>>>> prepare ${STAGE}: dropping bench_schema on destinations"
for db in "${DEST_DBS[@]}"; do
    psql -d "${db}" -qX -c "DROP SCHEMA IF EXISTS bench_schema CASCADE;"
done

case "${STAGE}" in
pre_data) ;;
post_data)
    echo ">>>>> prepare ${STAGE}: applying pre-data schema"
    schema_sync
    ;;
cutover)
    echo ">>>>> prepare ${STAGE}: applying pre-data and post-data schema"
    schema_sync
    schema_sync --data-sync-complete
    ;;
*)
    echo "prepare.sh: unknown stage '${STAGE}'" >&2
    exit 2
    ;;
esac
