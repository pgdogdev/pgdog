# Resharding benchmarks

Three suites measure different phases of the resharding pipeline:

| Suite | Script | What is timed |
|---|---|---|
| `copy_data` | `copy_data/run.sh` | Bulk COPY throughput, 3 source shards → 4 destination shards |
| `replication` | `replication/run.sh` | WAL streaming throughput draining a pre-seeded backlog |
| `schema_sync` | `schema_sync/run.sh` | Dump, parse and rewrite of the source schema per stage |

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

## Schema sync stages

Every stage runs `schema-sync --dry-run`. pgdog dumps the source schema,
parses it, rewrites it, and prints the statements of the stage. It writes
nothing to the destinations, so the numbers show pgdog cost, not Postgres DDL
cost.

| Stage | Flag | Statements produced |
|---|---|---|
| `pre_data` | none | Types, tables, defaults, table partitions, publication |
| `post_data` | `--data-sync-complete` | Indexes, constraints, foreign keys, index partitions |
| `cutover` | `--cutover` | One `setval` per sequence |

`BENCH_TABLES` scales the schema. The default is 500 tables per source shard.
Half of them use `integer` keys, which exercises the `integer` to `bigint`
rewrite.

```sh
# all three stages
bash benches/resharding/schema_sync/run.sh

# one stage, larger schema
BENCH_TABLES=1000 BENCH_STAGES=post_data bash benches/resharding/schema_sync/run.sh
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

The default toxics model a cross-region setup: pgdog next to the source, and the
destination in another region (for example us-east-1 → us-east-2). Latency is
one-way, so it adds the same amount to each round trip.

These environment variables change the toxics:

| Variable | Default | Effect |
|---|---|---|
| `SOURCE_LATENCY_MS` | `1` | Added latency on source reads |
| `DEST_LATENCY_MS` | `15` | Added latency on destination writes |
| `SOURCE_BW_KBPS` | `125000` | Bandwidth cap on source reads (KB/s per connection, `0` = no cap) |
| `DEST_BW_KBPS` | `125000` | Bandwidth cap on destination writes (KB/s per connection, `0` = no cap) |

```sh
# run with default toxic settings
USE_TOXI=1 bash benches/resharding/copy_data/run.sh

# simulate slow destination writes
DEST_BW_KBPS=5000 DEST_LATENCY_MS=20 USE_TOXI=1 bash benches/resharding/copy_data/run.sh

# compare proxied vs direct
bash benches/resharding/copy_data/run.sh --save-baseline direct
USE_TOXI=1 bash benches/resharding/copy_data/run.sh --baseline direct
```
