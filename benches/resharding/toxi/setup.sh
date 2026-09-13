#!/usr/bin/env bash
# Set up toxiproxy for resharding benchmarks.
#
# Four proxies, all upstream 127.0.0.1:5432:
#
#   Name                     Listen port   Used by
#   ──────────────────────────────────────────────
#   resharding_source        15400         primary entries (pgdog1, pgdog2, pgdog3)
#   resharding_replica_1     15410         copy worker entries (pgdog.toxi.toml)
#   resharding_replica_2     15411         copy worker entries (pgdog.toxi.toml)
#   resharding_destination   15401         shard_0, shard_1, shard_2, shard_3
#
# Toxics applied to each proxy (all configurable via env):
#   SOURCE_LATENCY_MS   -- latency on source reads, fast source proxies,       --downstream
#   SLOW_LATENCY_MS     -- latency on source reads, resharding_replica_2,      --downstream
#   SOURCE_BW_KBPS      -- bandwidth cap per connection, all source proxies,   --downstream
#   DEST_LATENCY_MS     -- latency on destination writes, --upstream
#   DEST_BW_KBPS        -- bandwidth cap on destination writes, --upstream
#
# Usage:
#   bash benches/resharding/toxi/setup.sh
#   SOURCE_LATENCY_MS=20 DEST_BW_KBPS=10000 bash benches/resharding/toxi/setup.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
CLI="${REPO_ROOT}/integration/toxiproxy-cli"
SERVER="${REPO_ROOT}/integration/toxiproxy-server"
UPSTREAM="127.0.0.1:5432"

SOURCE_LATENCY_MS="${SOURCE_LATENCY_MS:-0}"
DEST_LATENCY_MS="${DEST_LATENCY_MS:-0}"
SOURCE_BW_KBPS="${SOURCE_BW_KBPS:-10000}"
SLOW_LATENCY_MS="${SLOW_LATENCY_MS:-500}"
DEST_BW_KBPS="${DEST_BW_KBPS:-10000}"

# ── Start server ─────────────────────────────────────────────────────────────
echo "Starting toxiproxy-server..."
bash "${SCRIPT_DIR}/teardown.sh"
"${SERVER}" > /dev/null 2>&1 &

# Wait for API readiness.
until "${CLI}" list >/dev/null 2>&1; do sleep 0.2; done

# ── Helper ───────────────────────────────────────────────────────────────────
create_proxy() {
    local name="$1" port="$2"
    "${CLI}" delete "${name}" 2>/dev/null || true
    "${CLI}" create --listen "127.0.0.1:${port}" --upstream "${UPSTREAM}" "${name}"
}

# ── Proxies ───────────────────────────────────────────────────────────────────
create_proxy resharding_source      15400
create_proxy resharding_destination 15401
create_proxy resharding_replica_1  15410
create_proxy resharding_replica_2  15411

# ── Toxics ───────────────────────────────────────────────────────────────────
echo ""
echo "Applying toxics..."

for proxy in resharding_source resharding_replica_1; do
    "${CLI}" toxic add \
        --toxicName latency \
        --type      latency \
        --downstream \
        --attribute  latency="${SOURCE_LATENCY_MS}" \
        "${proxy}"
done

"${CLI}" toxic add \
    --toxicName latency \
    --type      latency \
    --downstream \
    --attribute  latency="${SLOW_LATENCY_MS}" \
    resharding_replica_2

for proxy in resharding_source resharding_replica_1 resharding_replica_2; do
    "${CLI}" toxic add \
        --toxicName bandwidth \
        --type      bandwidth \
        --downstream \
        --attribute  rate="${SOURCE_BW_KBPS}" \
        "${proxy}"
done

"${CLI}" toxic add \
    --toxicName latency \
    --type      latency \
    --upstream \
    --attribute  latency="${DEST_LATENCY_MS}" \
    resharding_destination

"${CLI}" toxic add \
    --toxicName bandwidth \
    --type      bandwidth \
    --upstream \
    --attribute  rate="${DEST_BW_KBPS}" \
    resharding_destination

echo ""
"${CLI}" list
echo ""
echo "Toxiproxy ready (API port=8474)."
echo "  fast source pipes latency=${SOURCE_LATENCY_MS}ms  bandwidth=${SOURCE_BW_KBPS} KB/s per connection"
echo "  slow replica      latency=${SLOW_LATENCY_MS}ms per round trip (resharding_replica_2)"
echo "  destination  latency=${DEST_LATENCY_MS}ms  bandwidth=${DEST_BW_KBPS} KB/s"
echo ""
echo "Run teardown.sh to stop, or just re-run setup.sh to restart."
