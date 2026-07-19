# Benchmarks

Performance benchmarks for PgDog. Results are stored in
`target/pgdog_benches/` and compared automatically between runs.

## Structure

```
benches/
  bench.sh          shared runner library
  compare.py        result comparison table
  resharding/       resharding benchmark suite
    pgdog.toml      config: 3 source shards (pgdog1-3) → 4 destination shards (shard_0-3)
    users.toml
    setup.sql       schema + tables + publication (run per source shard)
    fill.sql        synthetic data generator (BENCH_SCALE env var or \set scale)
    prepare.sh      hyperfine --prepare: drop destination schema, run schema-sync
    copy_data/
      run.sh        entry point: setup sources, then benchmark data-sync --sync-only
```

## Running

```sh
# Setup sources and benchmark (compares against the previous run automatically)
bash benches/resharding/copy_data/run.sh

# Save as a named baseline
bash benches/resharding/copy_data/run.sh --save-baseline main

# Compare against a named baseline (errors immediately if it does not exist)
bash benches/resharding/copy_data/run.sh --baseline main

# Fewer runs for a quick check
RUNS=1 WARMUP=0 bash benches/resharding/copy_data/run.sh
```

### Flow

1. **Setup** (once per invocation): drops and recreates `bench_copy` schema on each
   source shard (`pgdog1`, `pgdog2`, `pgdog3`), then fills each shard with its own
   slice of rows using a stepped `generate_series` keyed on `tenant_id`, so the source
   data is pre-distributed before the copy benchmark runs.
   Set `BENCH_SCALE=N` before running, or edit `\set scale` in `resharding/fill.sql` directly.

2. **Benchmark** (timed, N runs): hyperfine calls `prepare.sh` before each iteration
   (drops destination schema, runs `schema-sync`), then times `data-sync --sync-only`.
   This measures pure data-copy throughput with no WAL replication involved.

On first run there is no history yet:

```
No previous run found -- run again to see comparison.
```

On subsequent runs a comparison table is printed automatically:

```
copy_data

                  latest         current      change
  ----------  ----------  -------------  ----------
        mean    24.904 s       22.194 s     -10.88%
      median    24.100 s       21.900 s      -9.13%
         min    24.904 s       22.194 s     -10.88%
         max    24.904 s       22.194 s     -10.88%
      stddev      1.200 s        0.900 s     -25.00%
        user     2.226 s       11.234 s    +404.63%
      system   962.045ms        1.419 s     +47.50%
     max_mem      48.3 MB        46.1 MB      -4.55%

  Performance has improved.
```

`latest` is always the previous run. Named baselines persist in
`target/pgdog_benches/` until manually removed.

If a run is interrupted, the result file is left empty. The next run detects
this, skips the snapshot, and prints a notice instead of crashing. The run
after that resumes normal comparison automatically.

## Default approach: bench.sh + compare.py

`bench.sh` is a sourceable shell library that provides a single function:

```sh
bench_run NAME COMMAND [--prepare CMD] [--save-baseline NAME] [--baseline NAME]
```

It handles:
- Building pgdog in release mode (sets `PGDOG_BIN`, skipped if already set)
- Setting `RUST_LOG=error` to suppress log I/O overhead
- Running the command under [hyperfine](https://github.com/sharkdp/hyperfine)
- Passing `--prepare CMD` to hyperfine when provided (runs before each timed iteration)
- Saving results as JSON to `target/pgdog_benches/<name>.json`
- Snapshotting the previous result to `<name>.prev.json` before each run
- Calling `compare.py` to render the comparison table

`compare.py` reads two hyperfine JSON files and prints a Criterion-style table
with mean, median, min, max, stddev, user time, system time, and peak memory
(`max_mem` — peak of `memory_usage_byte` samples), relative change per field, and an overall pass/regression verdict. It is independent of `bench.sh`
and can be called directly:

```sh
python3 benches/compare.py target/pgdog_benches/before.json \
                            target/pgdog_benches/after.json \
                            --prev-label before --curr-label after
```

Benchmarks are not required to use this approach. A test can drive hyperfine
differently, use a different timing tool entirely, or produce its own output —
`bench.sh` is a convenience, not a contract.

## Adding a new benchmark

Create `benches/<suite>/run.sh`, source `bench.sh`, and call `bench_run`:

```sh
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${BENCH_DIR}/bench.sh"

bench_run "<name>" "<command to benchmark>" "$@"
```

To reset state between hyperfine iterations, pass `--prepare`:

```sh
bench_run "<name>" "<command>" --prepare "<reset command>" "$@"
```

The benchmarked script reads these variables from the environment at runtime,
so the same env vars control both the benchmark harness and the integration
test when run directly — the integration test simply uses its own defaults.
