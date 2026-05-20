#!/usr/bin/env bash
# Benchmark: copy_data — data-copy throughput (3 sources → 4 destination shards).
#
# Flow:
#   1. Setup: recreate bench_copy schema on each source shard and fill with data
#      directly via a stepped generate_series(:shard_index+1, :scale, :num_shards),
#      so each shard receives only its own slice of tenant_ids.
#   2. Benchmark: hyperfine runs data-sync --sync-only, calling prepare.sh
#      before each iteration (drops + resyncs destination schema).
#
# Usage:
#   bash benches/resharding/copy_data/run.sh [bench.sh flags]
#   bash benches/resharding/copy_data/run.sh --save-baseline main
#   bash benches/resharding/copy_data/run.sh --baseline main
#
# Tuning:
#   BENCH_SCALE=N  rows (inline tables); TOAST tables use scale/100
#   USE_TOXI=1     route pgdog through toxiproxy (started/stopped automatically)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BENCH_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${BENCH_DIR}/bench.sh"

export PGHOST=127.0.0.1
export PGPORT=5432
export PGUSER=pgdog
export PGPASSWORD=pgdog

SOURCE_DBS=(pgdog1 pgdog2 pgdog3)
NUM_SHARDS=${#SOURCE_DBS[@]}
export BENCH_SCALE=${BENCH_SCALE:-5000000}

PGDOG_CONFIG="${SETUP_DIR}/pgdog.toml"
if [[ "${USE_TOXI:-0}" == "1" ]]; then
    PGDOG_CONFIG="${SETUP_DIR}/pgdog.toxi.toml"
    # Register teardown before setup so a partial-setup failure still tears down.
    trap 'bash "${SETUP_DIR}/toxi/teardown.sh"' EXIT
    bash "${SETUP_DIR}/toxi/setup.sh"
fi
export PGDOG_CONFIG

echo "============================================================"
echo ">>>>> setup"
echo "============================================================"

for i in "${!SOURCE_DBS[@]}"; do
    db="${SOURCE_DBS[$i]}"
    echo ""
    echo "============================================================"
    echo ">>>>> [shard ${i}/${NUM_SHARDS}] ${db}: setup"
    echo "============================================================"
    psql -d "${db}" -qX -f "${SETUP_DIR}/setup.sql"
    psql -d "${db}" -qX \
        -v num_shards="${NUM_SHARDS}" \
        -v shard_index="${i}" \
        -f "${SETUP_DIR}/fill.sql"
done

echo ""
echo "============================================================"
echo ">>>>> benchmark"
echo "============================================================"
echo ""

DATA_COPY_CMD="\${PGDOG_BIN} \
    --config '${PGDOG_CONFIG}' \
    --users  '${SETUP_DIR}/users.toml' \
    data-sync \
    --sync-only \
    --from-database source \
    --to-database   destination \
    --publication   bench_copy \
    --replication-slot bench_copy_slot_copy"

bench_run "resharding.copy_data" "${DATA_COPY_CMD}" \
    --prepare "${SCRIPT_DIR}/prepare.sh" \
    "$@"
