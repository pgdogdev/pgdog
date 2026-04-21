# COPY_DATA Retry Reliability (Issues #894 + #897)

## Problem

**#894:** During `COPY_DATA` resharding, if a destination shard temporarily goes down, PgDog
continues processing the next tables without retrying. This leaves the destination incompletely
populated (276 started, 63 finished in the reported case).

**#897:** Two distinct failure scenarios need retry coverage:
1. **Destination shard is down** — connection to dest fails or drops mid-COPY
2. **Origin shard is down** — source connection drops, temporary replication slot is lost

The fix: add per-table retry logic with exponential backoff inside `ParallelSync`.

## Why a Single Top-Level Retry Handles Both #897 Scenarios

`Table::data_sync()` is fully self-contained:

```
data_sync() {
  CopySubscriber::new()  → dest connection
  ReplicationSlot::data_sync()  → fresh slot name (random_string(24)), temp slot
  slot.connect() + slot.create_slot()
  ... copy rows ...
  slot drops on disconnect (TEMPORARY)
}
```

On any failure and retry, `data_sync` is called from scratch:
- **Dest down** → new `CopySubscriber` reconnects to destination.
- **Origin down** → new `ReplicationSlot` with a fresh random name reconnects to source and
  re-creates the temporary slot. The old slot was `TEMPORARY` → auto-dropped by Postgres when
  its connection closed. No slot cleanup needed.

## Destination Commit Model

**How the copy works per-table:**
- Source side: `slot.create_slot()` opens `BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ`
  on the source. `COPY ... TO STDOUT` streams all rows from that consistent snapshot.
  `COMMIT` is called after all rows are streamed (source-only commit, unrelated to dest).
- Destination side: `CopySubscriber::connect()` opens one **standalone** connection per
  destination shard (no explicit `BEGIN`). `start_copy()` sends `COPY ... FROM STDIN` to
  each shard. Each `COPY FROM STDIN` runs inside PostgreSQL's own **implicit transaction**.
  `copy_done()` sends `CopyDone` to each shard **sequentially** — shard 0 commits, then
  shard 1, etc. **There is no cross-shard atomicity.**

**Failure scenarios and destination state:**

| Failure point | Destination state |
|---|---|
| During row streaming (conn drops mid-COPY) | PG auto-rolls back implicit tx → dest empty |
| Inside `copy_done()` — some shards committed, others not | Partially committed |
| After `copy_done()`, before `data_sync` returns `Ok` | All shards committed rows |

The common case (connection drop during streaming) leaves the destination clean — PostgreSQL
rolls back the implicit transaction automatically. The rare race is when `copy_done()` has
already committed on some or all shards and then the connection drops before `data_sync`
returns `Ok`. In that case, rows survive in the destination and a naive retry would immediately
hit primary key constraint violations.

**Current behavior — manual TRUNCATE guidance:**

PgDog does not automatically truncate the destination before retrying. Auto-TRUNCATE is the
correct long-term fix but requires reliable "is destination" guards that don't yet exist;
running TRUNCATE on the wrong cluster would be catastrophic. That logic is stubbed out as a
commented future extension in `run_with_retry()` in `parallel_sync.rs`.

Instead, when a table copy fails fatally (non-retryable error or max attempts exhausted),
`Table::destination_has_rows` queries each shard's primary with `SELECT 1 … LIMIT 1`. If
any rows are found, PgDog logs a `warn!` that includes the exact TRUNCATE statement to run:

```
data sync for "public"."orders" failed with rows remaining in destination;
truncate manually before retrying: TRUNCATE "public"."orders_new";
```

If the row-count check itself fails (destination unreachable), a separate warning is emitted.
The original error is always returned regardless.

**Non-retryable errors** (`CopyAborted`, `DataSyncAborted`, `NoPrimaryKey`, `NoReplicaIdentity`,
`ParallelConnection`) bypass the retry loop immediately and still trigger the row check.

## Code Path

```
admin/copy_data.rs           → Orchestrator::data_sync()
orchestrator.rs              → Publisher::data_sync()
publisher/publisher_impl.rs  → per-shard ParallelSyncManager::run()
publisher/parallel_sync.rs   → ParallelSync::run()           ← calls run_with_retry
                               ParallelSync::run_with_retry() ← retry loop (new)
publisher/table.rs           → Table::data_sync()            ← actual COPY (self-contained)
```

## Implementation

### 1. Config — add retry knobs (`pgdog-config/src/general.rs`)

Add after `resharding_parallel_copies` in the `General` struct:
```rust
/// Maximum number of retries for a failed table copy during resharding (per-table).
/// _Default:_ `5`
#[serde(default = "General::resharding_copy_retry_max_attempts")]
pub resharding_copy_retry_max_attempts: usize,

/// Delay in milliseconds between table copy retries. Doubles each attempt, capped at 32×.
/// _Default:_ `1000`
#[serde(default = "General::resharding_copy_retry_min_delay")]
pub resharding_copy_retry_min_delay: u64,
```

Private defaults and `impl Default` entries added following the existing `resharding_parallel_copies` pattern.

### 2. Cluster — propagate and expose (`pgdog/src/backend/pool/cluster.rs`)

Add both fields to `ClusterConfig<'a>` and the private `Cluster` struct; populate in
`ClusterConfig::new()` from `general.*`; wire through `Cluster::new()`; expose via:
```rust
pub fn resharding_copy_retry_max_attempts(&self) -> usize { ... }
pub fn resharding_copy_retry_min_delay(&self) -> &Duration { ... }
```

### 3. Table — helpers (`pgdog/src/backend/replication/logical/publisher/table.rs`)

```rust
/// Generate a TRUNCATE SQL statement for the given schema and table name.
pub fn truncate_statement(schema: &str, name: &str) -> String { ... }

/// Truncate this table on all destination primaries.
/// Not called automatically — preserved for future use once "is destination" guards exist.
pub async fn truncate_destination(&self, dest: &Cluster) -> Result<(), Error> { ... }

/// Returns true if any shard's primary has rows in the destination table.
/// Used after fatal failure to detect the COPY-committed-before-error race.
pub async fn destination_has_rows(&self, dest: &Cluster) -> Result<bool, Error> { ... }
```

### 4. Error — retryability predicate (`pgdog/src/backend/replication/logical/error.rs`)

Whitelist approach — only connection-level wrappers and direct availability variants return `true`.
New variants default to non-retryable, which is the safe choice.

```rust
pub fn is_retryable(&self) -> bool {
    match self {
        // Shard was unreachable; each retry opens a fresh connection.
        // Some sub-variants (TLS, protocol errors) aren't truly transient but
        // will just exhaust the budget and fail cleanly.
        Self::Net(_) | Self::Pool(_) => true,

        // No connection yet, or primary is down — worth retrying.
        Self::NotConnected | Self::NoPrimary => true,

        // Replication stalled; temporary slot is gone, next attempt starts fresh.
        Self::ReplicationTimeout => true,

        // Abort signals, schema mismatches, protocol violations — retrying won't help.
        _ => false,
    }
}
```

### 5. ParallelSync — retry loop (`pgdog/src/backend/replication/logical/publisher/parallel_sync.rs`)

Split `run()` into `run()` (public entry point, spawns task) and `run_with_retry()` (private).

On each retryable failed attempt:
1. Compute exponential backoff: `min_delay * 2^attempt`, capped at 32×.
2. Log the error and how long we are waiting, e.g. `failed (attempt 1/5): …, retrying after 500ms…`
3. Sleep for the backoff duration.
4. Increment attempt counter and loop.

On fatal failure (non-retryable error or attempts exhausted):
1. Record the error via `tracker.error()`.
2. Call `Table::destination_has_rows` — if rows are found, emit a `warn!` with the exact TRUNCATE SQL.
3. Return the original error to the caller.

## Configuration Reference

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `resharding_copy_retry_max_attempts` | `usize` | `5` | Maximum per-table retry attempts |
| `resharding_copy_retry_min_delay` | `u64` (ms) | `1000` | Base backoff delay in milliseconds; doubles each attempt, capped at 32× |

## Integration Test

`integration/copy_data/retry_test.sh` — stops shard_1 before the sync starts, brings it
back after ~2 s, asserts exit 0 and correct row counts on all tables.
Requires the `integration/copy_data/` docker-compose stack to be running.
Config: `integration/copy_data/pgdog.retry_test.toml` (faster retry settings for CI speed).
