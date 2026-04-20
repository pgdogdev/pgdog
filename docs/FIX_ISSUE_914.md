# Issue #914: `pool is shut down` during resharding COPY

## The issue

A large-scale reshard (276 tables, ~180 GB) completed its parallel COPY phase
successfully then emitted:

```
2026-04-18T02:31:14.592Z  ERROR [task: 1] pool: pool is shut down
```

The replication stream never started. The named replication slot
`__pgdog_repl_200ydsmxvvvbtco6kfu_0` remained inactive with a **2855 GB WAL
lag** on the source.

A second problem was also present: two sharded tables had no primary key.
`REPLICA IDENTITY FULL` had been applied as a workaround on the assumption that
pgdog would accept it. No validation error was raised before the copy started;
the failure would only have appeared after the full multi-hour copy completed.

---

## Footprints

### Log sequence

```
02:31:14.553Z  INFO  closing server connection [postgres@<shard0>:5440/db, reason: other]
02:31:14.553Z  INFO  closing server connection [postgres@<shard1>:5441/db, reason: other]
02:31:14.553Z  INFO  closing server connection [postgres@<shard2>:5442/db, reason: other]
02:31:14.558Z  INFO  closing server connection [postgres@<source>:5434/db, reason: closed by server]
02:31:14.564Z  INFO  table sync for 276 tables complete [db_name, shard: 0]
02:31:14.592Z  INFO  closing server connection [postgres@<source>:5434/db, state: idle, reason: other]
02:31:14.592Z  ERROR [task: 1] pool: pool is shut down
```

The lines at `.553Z` are the `CopySubscriber` shard connections closing normally
after the last table sync — expected.

The line at `.558Z` is the per-table replication slot connection closing after
draining the temporary WAL replay — expected.

The line at `.564Z` confirms all 276 tables finished successfully.

The line at `.592Z` with `reason: other` on a **source** connection is the
diagnostic. `DisconnectReason::Other` is the `#[default]` value, set when
`inner.rs::maybe_check_in()` finds `!self.online` and returns early without
recording a proper reason:

```rust
if !self.online && !moving || self.paused {
    result.replenish = false;
    return Ok(result);  // no disconnect_reason set → default = Other
}
```

This means a `Guard` (a checked-out connection) was being returned to a pool
that was **already offline**. The pool went offline while the Guard was in use,
not before `pool.get()` was called.

The error at `.592Z` is the immediately following `pool.get()` call on that
same offline pool.

### Task ID

`AsyncTasks` assigns monotonically increasing IDs starting at 0:

| ID | Task |
|---|---|
| 0 | `COPY_DATA` admin command |
| 1 | Replication waiter, registered inside COPY_DATA after `orchestrator.replicate()` returns |

`[task: 1]` means `orchestrator.replicate()` was called and the replication
`Waiter` was registered — then the waiter surfaced an error from inside the
spawned replication stream.

### How `pool is shut down` is produced

`pool::Error::Offline` (`error.rs`: `#[error("pool is shut down")] Offline`)
is emitted from five sites:

| Site | File | Condition |
|---|---|---|
| `Pool::get_internal()` fast path | `pool_impl.rs:132` | `pool.get()` called; `!guard.online` at lock time |
| `Waiting::new()` slow path | `waiting.rs:32` | No idle conn; tries to queue; `!guard.online` before queuing |
| Monitor request loop | `monitor.rs:110` | Monitor wakes; pool already offline; drains queued waiters |
| Monitor maintenance loop | `monitor.rs:200` | Periodic tick; `!guard.online`; drains waiters, stops monitor |
| `Pool::shutdown()` | `pool_impl.rs:365` | Called directly; drains all current waiters immediately |

The fast-path and slow-path sites are what a `pool.get()` caller observes as a
returned `Err`. The monitor sites drain waiters that were already queued when
the pool went offline between their enqueue and their wakeup.

### How a pool goes offline

Two mechanisms set `guard.online = false`:

**Mechanism A — `Pool::move_conns_to()`** (used on config reload). Sets
`from_guard.online = false`, migrates idle connections to the new pool, then
calls `shutdown()`. Checked-out connections are not forcibly closed; they reach
the new pool when their `Guard` is dropped.

**Mechanism B — `Pool::shutdown()`** (used on reconnect and as the final step
after migration):

```rust
pub fn shutdown(&self) {
    let mut guard = self.lock();
    guard.online = false;
    guard.dump_idle();                        // close idle connections now
    guard.close_waiters(Error::Offline);      // unblock queued waiters
    self.comms().shutdown.notify_waiters();
    self.comms().ready.notify_waiters();
}
```

### `replace_databases()` — the single dispatch point

Every reload path calls `databases.rs::replace_databases(new, reload)`. It
always marks the old pool generation offline:

```
replace_databases(new, reload=true)          ← config reload, DDL, cutover
  old.move_conns_to(&new)   → mechanism A: old pools offline, idle conns migrate
  new.launch()
  DATABASES.store(new)      → atomic global swap
  old.shutdown()            → mechanism B: any remaining old-pool handles offline

replace_databases(new, reload=false)         ← RECONNECT, init
  new.launch()
  DATABASES.store(new)
  old.shutdown()            → mechanism B only; no connection migration
```

After `DATABASES.store(new)`, `databases().get_cluster()` returns the new
generation. Any `Arc<Pool>` captured before the swap is still valid memory but
`guard.online == false`. Any subsequent `pool.get()` on it returns
`Error::Offline`.

### All triggers of `replace_databases`

```
Client DDL routed through pgdog
  query.rs: after transaction commit where cluster.reload_schema() == true
  (CreateStmt, DropStmt for table/index/view/sequence, ViewStmt,
   CreateTableAsStmt, AlterTableStmt set schema_changed = true on the Route)
    → schema_changed()
        → reload_from_existing()
            → replace_databases(reload=true)

Passthrough authentication
  databases::add() — new user or password change
    → reload_from_existing()
        → replace_databases(reload=true)

Resharding pre-data schema sync
  orchestrator::schema_sync_pre() — after pre-data DDL applied to destination
    → reload_from_existing()
        → replace_databases(reload=true)

Admin RELOAD / SIGHUP
  admin/reload.rs, net/tls.rs
    → reload()  (re-reads pgdog.toml)
        → replace_databases(reload=true)

Admin RECONNECT
  admin/reconnect.rs
    → reconnect()
        → replace_databases(reload=false)   ← no connection migration

Resharding cutover
  orchestrator::replicate_and_cutover() → databases::cutover()
    → replace_databases(reload=true)
```

---

## Exact bugs

### Bug 1 — Publisher holds a stale pool reference across a long COPY

`Publisher` is constructed during `schema_sync_pre()`. At that point
`schema_sync_pre()` calls `reload_from_existing()` (creating pool generation
**v2**) and the publisher stores a `Cluster` that wraps v2. This is correct at
construction time.

`data_sync()` runs for hours using only standalone connections and raw addresses
extracted at task start — it never calls `pool.get()` and is unaffected by pool
reloads.

During that window, any client DDL routed through pgdog calls
`reload_from_existing()` again, creating generation **v3** and marking v2
offline. The publisher still holds v2.

When `data_sync()` completes and `publisher.replicate()` is called:

```
publisher.replicate()
  sync_tables(false, dest)
    shard.primary(&Request::default()).await?    ← pool.get() on v2
                                                   v2.guard.online == false
                                                   → Error::Offline
                                                   → "pool is shut down"
```

The `reason: other` log entry at `.592Z` is the v2 `Guard` that was checked out
while v2 was still online, used during `Table::load()` (~28 ms), and then
returned to the now-offline v2 pool. The error immediately after is the next
`pool.get()` call hitting the same offline pool.

### Bug 2 — `valid()` called too late

`Table::valid()` checks that at least one column carries a replica identity:

```rust
pub fn valid(&self) -> Result<(), Error> {
    if !self.columns.iter().any(|c| c.identity) {
        return Err(Error::NoPrimaryKey(self.table.clone()));
    }
    Ok(())
}
```

Before this fix it was only called from `stream.rs` when the replication stream
processed the first row for a table — after the entire COPY phase had already
completed.

The column-level `identity` flag is set by the `COLUMNS` query via
`pg_get_replica_identity_index()`. For `REPLICA IDENTITY FULL`,
`pg_get_replica_identity_index()` returns `NULL`, the `LEFT JOIN` finds no
matching index, and **all columns get `identity=false`**. This means `valid()`
correctly returns `NoPrimaryKey` for a `REPLICA IDENTITY FULL` table — it does
not silently pass. The user in issue #914 would have seen this error eventually,
but only after the multi-hour copy had already completed and the replication
stream attempted to process the first row.

---

## Solution

### Fix 1 — Refresh the pool reference before replication starts

`Publisher::update_cluster(&Cluster)` replaces only `self.cluster`, leaving
`self.tables` (with per-table LSN watermarks) and `self.slots` untouched:

```rust
/// Replace the source cluster reference without disturbing accumulated
/// table LSN state or replication slot state.
///
/// Called after a long data_sync to pick up any pool reload that occurred
/// while the copy was running (e.g. triggered by a client DDL).
pub fn update_cluster(&mut self, cluster: &Cluster) {
    self.cluster = cluster.clone();
}
```

`Orchestrator::refresh_before_replicate()` re-fetches source and destination
from `databases()` (always the current live generation) and calls
`update_cluster()`:

```rust
pub(crate) async fn refresh_before_replicate(&mut self) -> Result<(), Error> {
    self.source = databases().schema_owner(&self.source.identifier().database)?;
    self.destination = databases().schema_owner(&self.destination.identifier().database)?;
    self.publisher.lock().await.update_cluster(&self.source);
    Ok(())
}
```

This is intentionally **not** `Orchestrator::refresh()`. `refresh()` recreates
the `Publisher` entirely, discarding the per-table LSN watermarks accumulated
during the copy. Those watermarks gate which WAL messages the replication stream
applies — discarding them causes the stream to re-apply all WAL from slot
creation, producing duplicates or conflicting updates. `update_cluster()` is
the minimal, safe swap.

`refresh_before_replicate()` is called at two points in the pipeline:

- `copy_data.rs` — between `orchestrator.data_sync().await?` and
  `orchestrator.replicate().await?` (the `COPY_DATA` admin command path)
- `orchestrator.rs::replicate_and_cutover()` — between `schema_sync_post()`
  and `replicate()` (the `RESHARD` command path)

### Fix 2 — Validate tables before the copy starts

`Publisher::data_sync()` now calls `table.valid()` on every table after
`sync_tables(true, dest)` resolves the table list but before any copy handle is
spawned:

```rust
// Validate all tables support replication before committing to
// what can be a multi-hour copy.  A table with no primary key or
// unique replica-identity index cannot be replicated correctly.
for tables in self.tables.values() {
    for table in tables {
        table.valid()?;
    }
}
```

`Error::NoPrimaryKey(table)` is returned immediately with the offending table
name. No data has moved, no replication slot has been created, and no cleanup is
required.

---

## Changed files

| File | Change |
|---|---|
| `pgdog/src/backend/replication/logical/publisher/publisher_impl.rs` | Add `Publisher::update_cluster()`; add `valid()` pre-check in `data_sync()` |
| `pgdog/src/backend/replication/logical/orchestrator.rs` | Add `Orchestrator::refresh_before_replicate()`; call it in `replicate_and_cutover()` |
| `pgdog/src/admin/copy_data.rs` | Call `refresh_before_replicate()` between `data_sync()` and `replicate()` |

---

## Guidance

**Can a primary key be added on the destination shards after schema sync but
before the copy?**

Yes. The safe window is between `schema_sync_pre()` completing and
`data_sync()` starting. Issue `ALTER TABLE … ADD PRIMARY KEY` on each
destination shard during that window. The source publication must then expose a
compatible replica identity — either a matching PK, or a unique index configured
with `ALTER TABLE … REPLICA IDENTITY USING INDEX`.

**Is a unique index sufficient instead of a primary key?**

Yes, if the table uses `ALTER TABLE t REPLICA IDENTITY USING INDEX unique_idx`.
That sets the indexed columns as the WAL replica identity. `valid()` passes, the
`ON CONFLICT` target is deterministic, and the upsert SQL is well-formed.

`REPLICA IDENTITY FULL` is not sufficient. `pg_get_replica_identity_index()`
returns `NULL` for FULL mode, so no columns are marked with `identity=true`
and `valid()` fails with `NoPrimaryKey` — the same error as a table with no
primary key at all. Use `REPLICA IDENTITY USING INDEX` instead.
