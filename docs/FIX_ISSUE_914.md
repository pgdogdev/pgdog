# Issue #914: `pool is shut down` during resharding COPY

[github.com/pgdogdev/pgdog/issues/914](https://github.com/pgdogdev/pgdog/issues/914)

A large-scale reshard (276 tables, ~180 GB) completed its parallel COPY phase
successfully then failed to start replication:

```
2026-04-18T02:31:14.592Z  ERROR [task: 1] pool: pool is shut down
```

The replication slot remained inactive with a 2855 GB WAL lag on the source.

A second problem: two tables used `REPLICA IDENTITY FULL` instead of a primary
key. No error was raised before the copy; it would only have appeared after the
multi-hour copy finished.

---

## Why COPY survived but replication didn't

The long-running copy phase of `data_sync` uses only pool-bypassing connections:

- **Source** — [`ReplicationSlot::data_sync`](../pgdog/src/backend/replication/logical/publisher/slot.rs) connects via raw `Address` →
  `Server::connect(...)` directly. The pool is not consulted.
- **Destination** — [`CopySubscriber::connect()`](../pgdog/src/backend/replication/logical/subscriber/copy.rs) calls `pool.standalone()`,
  which uses the pool as a config source only (address, credentials, TLS) and
  opens a raw `Server` connection. No `Guard` is issued, `pool.online` is never
  checked, and pool shutdown cannot reach these connections.

The metadata prelude of `data_sync` (`sync_tables` and `create_slots`) does call
`pool.get()` via `shard.primary()`, but this completes in seconds before the copy
loop begins. A RELOAD racing that prelude could still produce `Error::Offline`,
though in practice both operations are fast enough that the window is negligible.

When `replace_databases()` fires mid-copy — triggered by any client DDL,
passthrough auth event, or admin reload — it marks the old pool generation
offline (`guard.online = false`) and atomically swaps in a new one. The
already-open TCP connections used by the copy tasks are invisible to this;
they keep streaming rows.

The first `pool.get()` call in the post-copy path is inside `replicate()` →
`sync_tables()` → `shard.primary().await?`. If the cluster reference passed in
points to a now-offline pool generation, this is where `Error::Offline` hits.

---

## Root causes

**Bug 1 — stale cluster reference at replication start**

`Publisher` previously stored a `cluster: Cluster` field set at construction
time during `schema_sync_pre()`. Any pool reload during the multi-hour copy
created a new pool generation and marked the stored one offline. When
`replicate()` was called after the copy, it used the stale offline reference
for its first `pool.get()`.

**Bug 2 — `valid()` called too late**

`Table::valid()` checks that at least one column carries a replica identity.
Before this fix it was only called from `stream.rs` when the replication stream
processed the first WAL row — after the entire COPY had already completed.
`REPLICA IDENTITY FULL` tables always fail `valid()` because
`pg_get_replica_identity_index()` returns `NULL` for FULL mode, leaving all
columns with `identity=false`. The error was always going to appear; it just
appeared at the worst possible time.

A secondary ordering bug was also present: `create_slots()` ran before
`valid()`, so a `NoPrimaryKey` failure left orphaned replication slots on the
source that required manual cleanup.

---

## Fix

**Fix 1 — Remove the `cluster` field from `Publisher` entirely**

Source and destination clusters are now held exclusively on `Orchestrator` and
passed as `&Cluster` parameters to `publisher.data_sync(...)` and
`publisher.replicate(...)` on each call. The publisher cannot hold a stale
reference because it no longer holds one at all.

[`Orchestrator::refresh()`](../pgdog/src/backend/replication/logical/orchestrator.rs) re-fetches both clusters from `databases()` —
the live atomic store — immediately before `replicate()` is called. This is
done at two call sites: [`copy_data.rs`](../pgdog/src/admin/copy_data.rs) (the `COPY_DATA` admin command path)
and `replicate_and_cutover()` in [`orchestrator.rs`](../pgdog/src/backend/replication/logical/orchestrator.rs) (the `RESHARD` path).

`refresh_publisher()` is intentionally kept separate. It recreates the
`Publisher` entirely, discarding the per-table LSN watermarks accumulated
during the copy. Discarding those watermarks would cause the replication stream
to re-apply all WAL from slot creation and produce duplicates.
`refresh_publisher()` is only called at construction time and after cutover,
where a clean slate is correct.

**Fix 2 — Validate tables before the copy starts**

[`Publisher::data_sync()`](../pgdog/src/backend/replication/logical/publisher/publisher_impl.rs)
now runs `sync_tables()` first (column metadata that `valid()` reads), then
`valid()` (rejects any table with no replica identity before touching resources),
then `create_slots()`. The ordering matters: a `NoPrimaryKey` error raised
before slot creation leaves no orphaned slots on the source that would require
manual cleanup. `Error::NoPrimaryKey` is returned with no data moved.

---

## Future risks

**`pool.get()` inside the copy path will silently break on pool reload.**
The long-running copy loop of `data_sync` is immune to pool reloads because
it uses only `pool.standalone()` or raw `Address` connections. The short
metadata prelude (`sync_tables`, `create_slots`) does call `pool.get()` but
completes in seconds. Any future code that calls `pool.get()` inside the copy
loop will work under normal conditions and fail only when a pool reload races
a long copy in production. All connections opened during the copy loop must
use `pool.standalone()` or an existing `Server` handle.

**`refresh()` is a point-in-time snapshot with a residual race window.**
A pool reload between `refresh()` returning and the first `shard.primary()`
call inside `replicate()` would produce the same error. The window is
microseconds in practice. The structural fix is to fetch from `databases()` at
each `pool.get()` call site rather than caching clusters on `Orchestrator` at
all — but that is a larger refactor.

**`standalone()` connections have no reconnect.**
A dropped `standalone()` connection mid-copy fails the entire copy task with no
retry. The `lsn` field on `Table` (`publisher/table.rs`) records the WAL
position reached by the copy; resume-from-LSN is structurally possible without
restarting the full run, but is not yet implemented.

---

## Guidance

**Can a primary key be added after schema sync but before the copy?**
Yes. The safe window is between `schema_sync_pre()` completing and
`data_sync()` starting. Issue `ALTER TABLE … ADD PRIMARY KEY` on each
destination shard. The source publication must expose a compatible replica
identity — a matching PK or a unique index via
`ALTER TABLE … REPLICA IDENTITY USING INDEX`.

**Is a unique index sufficient?**
Yes, with `ALTER TABLE t REPLICA IDENTITY USING INDEX unique_idx`. The indexed
columns become the WAL replica identity, `valid()` passes, and the upsert
target is deterministic.

**`REPLICA IDENTITY FULL` is not sufficient.** `pg_get_replica_identity_index()`
returns `NULL` for FULL mode; no columns get `identity=true` and `valid()`
fails with `NoPrimaryKey`. Use `REPLICA IDENTITY USING INDEX` instead.
