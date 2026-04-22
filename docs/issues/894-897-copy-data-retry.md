# COPY_DATA Retry Reliability (Issues #894 + #897)

**#894:** During `COPY_DATA` resharding, if a destination shard temporarily went down, PgDog
continued processing the next tables without retrying. The destination was left incompletely
populated (276 started, 63 finished in the reported case).

**#897:** Two failure modes need coverage:
1. **Destination shard is down** — connection to dest fails or drops mid-COPY
2. **Origin shard is down** — source connection drops, temporary replication slot is lost

---

## Solution

### Retry mechanism

`Table::data_sync()` in `publisher/table.rs` opens fresh connections on every call, so a
single retry loop covers both failure modes:

- **Dest down** → new `CopySubscriber` reconnects to destination.
- **Origin down** → new `ReplicationSlot` with a fresh random name. The old slot was
  `TEMPORARY` and was auto-dropped by Postgres when its connection closed.

The retry loop lives in `ParallelSync::run_with_retry()` (`publisher/parallel_sync.rs`):
exponential backoff starting at `resharding_copy_retry_min_delay`, doubling each attempt,
capped at 32×, up to `resharding_copy_retry_max_attempts` tries.

| Key | Default | Description |
|-----|---------|-------------|
| `resharding_copy_retry_max_attempts` | `5` | Per-table retry attempts |
| `resharding_copy_retry_min_delay` | `1000` ms | Base backoff; doubles each attempt, capped at 32× |

### Truncation

Each table COPY runs inside PostgreSQL's own implicit transaction — no explicit `BEGIN`.
There is no cross-shard atomicity.

| Failure point | Destination state |
|---|---|
| During row streaming | PG rolls back implicit tx → dest empty |
| Inside `copy_done()` — some shards committed, others not | Partially committed |
| After `copy_done()`, before `data_sync` returns `Ok` | All shards have rows |

The common case (drop during streaming) leaves the destination clean. The rare race is a
commit that landed before the connection dropped — a retry then hits primary key violations.

PgDog does not auto-TRUNCATE before retrying (wrong-cluster risk). On fatal failure,
`Table::destination_has_rows` checks each shard and logs a `warn!` with the exact `TRUNCATE`
statement to run manually. Auto-truncate is stubbed as a future extension in `run_with_retry()`.

### Error handling

`is_retryable()` in `error.rs` uses a whitelist: `Net`, `Pool`, `NotConnected`, `NoPrimary`,
and `ReplicationTimeout` return `true`; everything else defaults to `false`. Non-retryable
errors (`CopyAborted`, `DataSyncAborted`, `NoPrimaryKey`, `NoReplicaIdentity`) bypass the
retry loop immediately but still trigger the destination row check.

### Tests

`integration/copy_data/retry_test/run.sh` kills shard_1 mid-sync, brings it back after ~2 s,
and asserts exit 0 with correct row counts. Uses faster retry settings in
`retry_test/pgdog.toml` for CI speed.
