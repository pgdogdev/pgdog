#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BENCH_DIR="$(cd "${SETUP_DIR}/.." && pwd)"

source "${BENCH_DIR}/bench.sh"

export PGHOST=127.0.0.1
export PGPORT=5432
export PGUSER=pgdog
export PGPASSWORD=pgdog
export PGOPTIONS="-c client_min_messages=warning"

SOURCE_DBS=(pgdog1 pgdog2 pgdog3)
NUM_SHARDS=${#SOURCE_DBS[@]}
export BENCH_TABLES=${BENCH_TABLES:-500}
BENCH_STAGES=${BENCH_STAGES:-"pre_data post_data cutover"}

PGDOG_CONFIG="${SETUP_DIR}/pgdog.toml"
if [[ "${USE_TOXI:-0}" == "1" ]]; then
    PGDOG_CONFIG="${SETUP_DIR}/pgdog.toxi.toml"
    trap 'bash "${SETUP_DIR}/toxi/teardown.sh"' EXIT
    bash "${SETUP_DIR}/toxi/setup.sh"
fi
export PGDOG_CONFIG

echo ">>>>> setup: ${BENCH_TABLES} tables on ${NUM_SHARDS} source shards"

for db in "${SOURCE_DBS[@]}"; do
    psql -d "${db}" -qX \
        -v num_tables="${BENCH_TABLES}" \
        -f "${SCRIPT_DIR}/schema.sql" >/dev/null
done

stage_flags() {
    case "$1" in
    pre_data) echo "" ;;
    post_data) echo "--data-sync-complete" ;;
    cutover) echo "--cutover" ;;
    *)
        echo "run.sh: unknown stage '$1'" >&2
        exit 2
        ;;
    esac
}

for stage in ${BENCH_STAGES}; do
    flags="$(stage_flags "${stage}")"

    echo ""
    echo ">>>>> benchmark: schema_sync ${stage} (dry run)"

    DRY_RUN_CMD="\${PGDOG_BIN} \
        --config '${PGDOG_CONFIG}' \
        --users  '${SETUP_DIR}/users.toml' \
        schema-sync \
        --from-database source \
        --to-database   destination \
        --publication   bench_schema \
        --dry-run \
        ${flags} >/dev/null"

    bench_run "resharding.schema_sync.${stage}.dry_run" "${DRY_RUN_CMD}" "$@"
done
