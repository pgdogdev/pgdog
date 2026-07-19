# Resharding benchmarks

Two suites measure different phases of the resharding pipeline:

| Suite | Script | What is timed |
|---|---|---|
| `copy_data` | `copy_data/run.sh` | Bulk COPY throughput, 3 source shards → 4 destination shards |
| `replication` | `replication/run.sh` | WAL streaming throughput draining a pre-seeded backlog |

## Prerequisites

- Running Postgres with the databases defined in `pgdog.toml`
- `PGDOG_BIN` pointing at the pgdog binary
- `bench.sh` in the parent directory (hyperfine wrapper)

## Running

```sh
# copy throughput
bash benches/resharding/copy_data/run.sh

# WAL replication throughput
bash benches/resharding/replication/run.sh

# scale up the dataset
BENCH_SCALE=10000000 bash benches/resharding/copy_data/run.sh

# save a baseline then compare
bash benches/resharding/copy_data/run.sh --save-baseline main
bash benches/resharding/copy_data/run.sh --baseline main
```

## Network simulation with toxiproxy

Pass `USE_TOXI=1` to any suite to route pgdog connections through
[toxiproxy](https://github.com/Shopify/toxiproxy). The run script starts
the proxy automatically before the bench and tears it down on exit.

Two proxies are created:

| Proxy | Port | Shards | Direction |
|---|---|---|---|
| `resharding_source` | 15400 | pgdog1, pgdog2, pgdog3 | downstream (PG → pgdog) |
| `resharding_destination` | 15401 | shard_0–shard_3 | upstream (pgdog → PG) |

Toxic settings are configurable via environment variables:

| Variable | Effect |
|---|---|
| `SOURCE_LATENCY_MS` | Added latency on source reads |
| `DEST_LATENCY_MS` | Added latency on destination writes |
| `SOURCE_BW_KBPS` | Bandwidth cap on source reads (KB/s) |
| `DEST_BW_KBPS` | Bandwidth cap on destination writes (KB/s) |

```sh
# run with default toxic settings
USE_TOXI=1 bash benches/resharding/copy_data/run.sh

# simulate slow destination writes
DEST_BW_KBPS=5000 DEST_LATENCY_MS=20 USE_TOXI=1 bash benches/resharding/copy_data/run.sh

# compare proxied vs direct
bash benches/resharding/copy_data/run.sh --save-baseline direct
USE_TOXI=1 bash benches/resharding/copy_data/run.sh --baseline direct
```
