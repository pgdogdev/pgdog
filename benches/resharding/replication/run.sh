#!/usr/bin/env bash
# Benchmark: WAL replication throughput (3 sources → 4 destination shards).
#
# Measures time for pgdog to drain a pre-seeded WAL backlog via --replicate-only.
# COPY happens in prepare (untimed); prepare also writes per-source sentinel rows.
# The timed command streams WAL and exits once all sentinels appear on all shards.
#
# Usage:
#   bash benches/resharding/replication/run.sh [bench.sh flags]
#   bash benches/resharding/replication/run.sh --save-baseline main
#   bash benches/resharding/replication/run.sh --baseline main
#
# Tuning:
#   BENCH_SCALE=N    sessions per source shard; TOAST rows = N/100
#   RUNS=N WARMUP=N  forwarded to hyperfine via environment
#   USE_TOXI=1       route pgdog through toxiproxy (started/stopped automatically)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BENCH_DIR="$(cd "${SETUP_DIR}/.." && pwd)"

source "${BENCH_DIR}/bench.sh"

export PGHOST=127.0.0.1
export PGPORT=5432
export PGUSER=pgdog
export PGPASSWORD=pgdog
export SETUP_DIR  # inherited by prepare.sh

DEST_DBS=(shard_0 shard_1 shard_2 shard_3)
export BENCH_SCALE=${BENCH_SCALE:-50000}

PGDOG_CONFIG="${SETUP_DIR}/pgdog.toml"
if [[ "${USE_TOXI:-0}" == "1" ]]; then
    PGDOG_CONFIG="${SETUP_DIR}/pgdog.toxi.toml"
    # Register teardown before setup so a partial-setup failure still tears down.
    trap 'bash "${SETUP_DIR}/toxi/teardown.sh"' EXIT
    bash "${SETUP_DIR}/toxi/setup.sh"
fi
export PGDOG_CONFIG

echo "============================================================"
echo ">>>>> benchmark"
echo "============================================================"
bench_run "resharding.streaming" \
    "${SCRIPT_DIR}/stream.sh" \
    --prepare "${SCRIPT_DIR}/prepare.sh" \
    "$@"
