#!/usr/bin/env bash
# hyperfine --prepare: reset destination shards and re-sync schema.
#
# Runs before every timed data-copy iteration:
#   1. Drop inactive replication slots on all source shards.
#   2. Drop bench_copy schema on all 4 destination shards.
#   3. Re-create it via pgdog schema-sync.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# PGDOG_BIN and PGDOG_CONFIG are exported by run.sh; this script is not standalone.
PGDOG_CONFIG="${PGDOG_CONFIG:-${SETUP_DIR}/pgdog.toml}"

export PGHOST=127.0.0.1
export PGPORT=5432
export PGUSER=pgdog
export PGPASSWORD=pgdog

SOURCE_DBS=(pgdog1 pgdog2 pgdog3)
DEST_DBS=(shard_0 shard_1 shard_2 shard_3)

echo "============================================================"
echo ">>>>> prepare"
echo "============================================================"

echo "============================================================"
echo ">>>>> [1/3] dropping inactive replication slots (sources)"
echo "============================================================"
for db in "${SOURCE_DBS[@]}"; do
    psql -d "${db}" -qX -c \
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'bench_copy%' AND NOT active;"
done

echo ""
echo "============================================================"
echo ">>>>> [2/3] dropping bench_copy schema (destinations)"
echo "============================================================"
for db in "${DEST_DBS[@]}"; do
    psql -d "${db}" -qX -c "DROP SCHEMA IF EXISTS bench_copy CASCADE;"
done

echo ""
echo "============================================================"
echo ">>>>> [3/3] schema-sync: source → destination"
echo "============================================================"
"${PGDOG_BIN}" \
    --config "${PGDOG_CONFIG}" \
    --users  "${SETUP_DIR}/users.toml" \
    schema-sync \
    --from-database source \
    --to-database   destination \
    --publication   bench_copy
echo "============================================================"
echo ">>>>> schema-sync done"
echo "============================================================"
echo ""
