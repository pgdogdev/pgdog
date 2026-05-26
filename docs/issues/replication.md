# Replication Issues

---

## 🚧 Issue 1 — Lag inflation when source databases share a PostgreSQL instance

### Description

The replication lag metric used to gate cutover is computed as:

```sql
SELECT pg_current_wal_lsn() - confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name = '...';
```

`pg_current_wal_lsn()` returns the current write-ahead log position for the **entire PostgreSQL instance**, not for a specific database or publication. When multiple source shards are hosted on the same PostgreSQL instance (different databases, one slot per database), `pg_current_wal_lsn()` advances with every write to any database on that instance.

A logical replication slot only decodes and delivers changes that belong to its publication. Changes from other databases are physically present in the WAL stream but are invisible to the slot's decoder — `confirmed_flush_lsn` only advances when the client acknowledges a decoded logical message (i.e., a `Commit` record from the publication). Once a slot has replayed all of its publication's data, `confirmed_flush_lsn` stagnates at the LSN of the last commit in that publication. It will never advance past WAL records from other databases, regardless of whether writes have stopped.

This means the lag metric permanently overstates the remaining work. On a three-shard benchmark where all source databases are on one instance, the observed lag was ~3.5 GB per slot even after each slot had replayed all of its own publication's data. The lag never dropped below the cutover threshold, so `wait_for_replication()` looped indefinitely and cutover never fired.

### Cause

`pg_current_wal_lsn()` is instance-scoped. `confirmed_flush_lsn` is publication-scoped. Their difference is only meaningful when a single database accounts for all writes to the instance.

### Code references

| Symbol | File |
|---|---|
| `ReplicationSlot::replication_lag()` — the lag query | [`pgdog/src/backend/replication/logical/publisher/slot.rs`](../../pgdog/src/backend/replication/logical/publisher/slot.rs) |
| `ReplicationWaiter::wait_for_replication()` — the cutover gate | [`pgdog/src/backend/replication/logical/orchestrator.rs`](../../pgdog/src/backend/replication/logical/orchestrator.rs) |
| Keepalive handler / `flush_lsn` reply (`data_since_keepalive` flag) | [`pgdog/src/backend/replication/logical/publisher/publisher_impl.rs`](../../pgdog/src/backend/replication/logical/publisher/publisher_impl.rs) |
### Fix

The PostgreSQL walsender sends a keepalive message after exhausting all decoded changes available for the publication. The keepalive carries `wal_end` — the server's current WAL write position. When a keepalive arrives and no xlog data was received since the previous keepalive, the slot has drained: there is nothing for the publication between `committed_lsn` and `wal_end`, so that gap consists entirely of other databases' WAL.

In this state the client can safely reply with `flush_lsn = wal_end`. PostgreSQL sets `confirmed_flush_lsn` on the slot to `wal_end`, and the lag query returns ~0.

Reporting `wal_end` is only valid when the slot is caught up. During active streaming — where the server is sending transactions and keepalives may arrive between commits — `flush_lsn` must remain at `committed_lsn`. Reporting `wal_end` prematurely would advance `confirmed_flush_lsn` past unapplied commits; if the connection dropped, the server would restart from `wal_end` and those commits would be lost.

The implemented guard: `data_since_keepalive` flag. Set to `true` when any xlog data message arrives; cleared to `false` when a keepalive arrives. A keepalive is the catch-up signal only when this flag is `false` — meaning no data arrived between the last keepalive and this one.

```
keepalive received
  data_since_keepalive = true  -> reply flush_lsn = committed_lsn  (active, mid-stream)
  data_since_keepalive = false -> reply flush_lsn = wal_end         (caught up)
```

### PostgreSQL references

- [Streaming Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html) — Standby Status Update message (`flush_lsn` field); Primary Keepalive message (`wal_end` field).
- [`pg_replication_slots`](https://www.postgresql.org/docs/current/view-pg-replication-slots.html) — `confirmed_flush_lsn` column: the last LSN confirmed received by the standby/subscriber.
- [Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html) — publication filtering and how the walsender decodes only changes relevant to the subscriber's publication.

---

## 🚧 Issue 2 — Stop signal only unblocked one task instead of all

### Description

When cutover initiates, `Waiter::stop()` must signal all N per-shard replication tasks to terminate. Each task blocks in a `select!` loop waiting on either incoming WAL data or a stop signal. All N tasks must receive the signal and break out of their loop before `Waiter::wait()` can return.

The original implementation used `Arc<Notify>` for the stop signal. `Notify::notify_one()` wakes exactly one waiting task. `Notify::notify_waiters()` wakes only tasks that are *currently parked* on the future at the moment of the call — any task that polls `notified()` after `notify_waiters()` returns will park again and never wake. With N tasks the result was: one task exited, the others remained blocked indefinitely. `Waiter::wait()` joined all task handles and hung.

### Cause

`Notify` does not persist state. A permit stored by `notify_one()` is consumed by the first task that calls `notified().await`; subsequent tasks see no permit and park. `notify_waiters()` is a snapshot operation: it only affects tasks already parked at the instant of the call.

### Code references

| Symbol | File |
|---|---|
| `Publisher::stop` / `Waiter::stop` — the stop channel | [`pgdog/src/backend/replication/logical/publisher/publisher_impl.rs`](../../pgdog/src/backend/replication/logical/publisher/publisher_impl.rs) |
| `Waiter::stop()` — sends the signal | same file |
| `stop_rx.changed()` arm in per-shard task `select!` | same file |
### Fix

Replace `Arc<Notify>` with `Arc<watch::Sender<bool>>` / `watch::Receiver<bool>`. `watch::Sender::send(true)` persists the value in the channel. Every receiver — whether currently parked or polling later — resolves `changed()` as soon as it observes the new value. One `send(true)` call unblocks all N tasks regardless of scheduling order.

```rust
// Publisher and Waiter both hold Arc<watch::Sender<bool>>.
// Each task subscribes before spawning:
let mut stop_rx = self.stop.subscribe(); // watch::Receiver<bool>

// Inside the task select!:
_ = stop_rx.changed() => {
    slot.stop_replication().await?;
    break;
}

// Waiter::stop() -- one call, all tasks unblock:
pub fn stop(&self) {
    let _ = self.stop.send(true);
}
```

### Tokio references

- [`tokio::sync::Notify`](https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html) — `notify_one()` stores at most one permit; `notify_waiters()` is not persistent.
- [`tokio::sync::watch`](https://docs.rs/tokio/latest/tokio/sync/watch/index.html) — `Sender::send()` updates the shared value; all receivers observe it on their next `changed()` poll.

---

## 🚧 Issue 3 — Premature cutover when lag map is empty at startup

### Description

The orchestrator computes the current replication lag as the maximum value across all per-shard lag entries:

```rust
lag.values().copied().max().unwrap_or(i64::MAX) as u64
```

Per-shard lag values are written into `replication_lag: HashMap<usize, i64>` by each task's `check_lag.tick()` interval. This interval fires once per second, but the first tick does not fire until one second after the task spawns.

The orchestrator's `wait_for_replication()` loop runs immediately after `replicate()` returns. If it evaluates `replication_lag()` before any task's first tick, the map is empty. `Iterator::max()` on an empty iterator returns `None`. The original code used `unwrap_or_default()`, which returns `0`. The orchestrator saw lag = 0 bytes, concluded replication was already caught up, entered maintenance mode, and triggered cutover before any data had been replicated.

### Cause

Two independent races:
1. `unwrap_or_default()` on an empty map returned 0, which is indistinguishable from a legitimately zero lag.
2. No synchronization between task startup and the first lag measurement.

### Code references

| Symbol | File |
|---|---|
| `Publisher::replicate()` — pre-population loop for `replication_lag` map | [`pgdog/src/backend/replication/logical/publisher/publisher_impl.rs`](../../pgdog/src/backend/replication/logical/publisher/publisher_impl.rs) |
| `Publisher::replication_lag()` — reads the map | same file |
| `Orchestrator::replication_lag()` — takes the max across shards | [`pgdog/src/backend/replication/logical/orchestrator.rs`](../../pgdog/src/backend/replication/logical/orchestrator.rs) |
### Fix

Pre-populate the map with `i64::MAX` for every shard index before spawning any tasks. Use `or_insert` so a real measurement written by a task is never overwritten by the sentinel. Change `unwrap_or_default()` to `unwrap_or(i64::MAX)` so an empty map (which should not occur after pre-population) is also treated as maximally lagged rather than zero.

```rust
// Before spawning tasks:
let mut guard = self.replication_lag.lock();
for number in 0..n_sources {
    guard.entry(number).or_insert(i64::MAX);
}

// Orchestrator reads:
lag.values().copied().max().unwrap_or(i64::MAX) as u64
```

The sentinel is overwritten by the first real measurement from each task's `check_lag.tick()`. Until that point the orchestrator treats every uninitialized shard as having infinite lag and will not proceed.

---

## 🚧 Issue 4 — Divergent code paths for the same operation

### Description

The resharding pipeline — schema sync, data sync, replication, cutover — can be initiated through four independent entry points:

| Entry point | Phase coverage |
|---|---|
| `RESHARD` admin command | full flow: schema sync, data sync, replication, cutover |
| CLI `replicate-and-cutover` | same full flow |
| `REPLICATE` admin command | replication only; schema/data sync must already be done |
| `REPLICATE` + `CUTOVER` admin commands | replication started as background task; cutover triggered externally |

Each path composes the same underlying primitives (`Orchestrator`, `ReplicationWaiter`, `Publisher`) but assembles them differently and makes different assumptions about which phases have already run and which signals will arrive. When one path is fixed, the fix is not applied to the others because there is no single place that owns the shared contract.

The concrete instance that surfaced during debugging: the background task registered by `REPLICATE` exits without performing cutover when the slot drains naturally. It waits in:

```rust
select! {
    _ = abort_rx           => { waiter.stop(); }
    _ = cutover.notified() => { waiter.cutover().await; }
    result = waiter.wait() => { /* log error only */ }
}
```

When the source slot drains (`CopyDone`), `waiter.wait()` returns and the task exits via the third arm. `waiter.cutover()` is never called: `wait_for_replication()`, `wait_for_cutover()`, `schema_sync_post_cutover()`, and `databases::cutover()` (which flips traffic) all go unexecuted. The destination is fully populated and replication has stopped, but pgdog still routes to the source. The direct paths (`RESHARD`, CLI) always call `cutover()` at the end — they do not have this gap.

A secondary consequence of path divergence: `AsyncTasks::cutover()` still calls `notify_one()` on `Arc<Notify>` (the primitive replaced in Issue 2). The fix in `publisher_impl.rs` was not propagated to the admin layer. This creates a race in the `REPLICATE` + `CUTOVER` path: if the slot drains at the same instant the operator sends `CUTOVER`, `select!` picks one arm non-deterministically and the notification may be silently discarded.

### Cause

Each entry point was built to satisfy a specific operational need without consolidating around a shared flow. There is no contract enforcing which phases run in which order, so paths accumulate independent deviations over time.

### Code references

| Symbol | File |
|---|---|
| `TaskType::Replication` `select!` / `waiter.wait()` arm (line ~150) | [`pgdog/src/backend/replication/logical/admin.rs`](../../pgdog/src/backend/replication/logical/admin.rs) |
| `AsyncTasks::cutover()` / `notify_one()` (line ~75) | same file |
| `Replicate::execute()` | [`pgdog/src/admin/replicate.rs`](../../pgdog/src/admin/replicate.rs) |
| `Reshard::execute()` | [`pgdog/src/admin/reshard.rs`](../../pgdog/src/admin/reshard.rs) |
| `Orchestrator::replicate_and_cutover()` — the canonical flow | [`pgdog/src/backend/replication/logical/orchestrator.rs`](../../pgdog/src/backend/replication/logical/orchestrator.rs) |
### Fix

Two independent fixes are needed.

#### Immediate fix — cutover on natural drain

The `waiter.wait()` arm in the background task `select!` currently only logs errors; on `Ok(())` it exits silently without performing cutover. The slot has fully drained, the destination is populated, but pgdog still routes traffic to the source. The fix is to call `waiter.cutover()` on successful completion:

```rust
result = waiter.wait() => {
    match result {
        Ok(()) => {
            // Slot drained naturally — still perform cutover.
            if let Err(err) = waiter.cutover().await {
                error!(...);
            }
        }
        Err(err) => error!(...),
    }
}
```

#### Secondary fix — `notify_one()` race in `AsyncTasks::cutover()`

`AsyncTasks::cutover()` still calls `notify_one()` on `Arc<Notify>` (the primitive replaced in Issue 2 for the stop signal). The fix from Issue 2 was not propagated to the admin layer. This creates a race in the `REPLICATE` + `CUTOVER` path: if the slot drains at the same instant the operator sends `CUTOVER`, `select!` picks one arm non-deterministically and the notification may be silently discarded.

Replace `Arc<Notify>` with `watch::Sender<bool>` in `AsyncTasks` so that `cutover.send(true)` persists the value and any task polling `cutover.changed()` — whether parked or not at the moment of the call — sees it.

#### Structural fix

Make `Orchestrator::replicate_and_cutover()` the single canonical implementation of the full flow and have the background-task path call it rather than assembling phases independently. The background-task model's only responsibility should be *when* to trigger cutover (immediately, on external signal, on timeout) — not *how* to execute it.

### References

- [Tokio `select!` macro](https://docs.rs/tokio/latest/tokio/macro.select.html) — when multiple branches are ready simultaneously, one is chosen pseudo-randomly; no branch is guaranteed to execute.

---

## 🚧 Issue 5 — `AbortSignal` is not an abort signal; it is a coordinator-gone detector

### Description

`AbortSignal` is used inside the parallel table-copy path to interrupt in-flight `COPY` loops when the sync coordinator exits. The name implies an active cancellation primitive, but the mechanism is entirely passive: it wraps an `UnboundedSender` and calls `tx.closed().await`, which resolves only when the corresponding `rx` (owned by `ParallelSyncManager::run()`) is dropped.

There is no `abort()` method. Nothing sends a signal. The only way the future resolves is if the receiver end of the channel is dropped — which happens as a side effect of the manager returning, not as an intentional cancellation act.

This creates four concrete problems.

**1. `rx` is only dropped when `manager.run()` returns.**

`ParallelSyncManager::run()` is called inside a `tokio::spawn`ed task. That task runs independently of its caller. Dropping or cancelling `Orchestrator::data_sync()` or `Publisher::data_sync()` mid-await does not cancel the spawned task, does not drop `rx`, and does not fire the abort signal in any worker. Workers keep running until `manager.run()` finishes on its own.

**2. The only trigger for `rx` dropping mid-run is a worker error propagating through `?`.**

Inside `run()`:

```rust
while let Some(table) = rx.recv().await {
    tables.push(table?);  // ← error here short-circuits the loop
};
```

When one worker sends `Err(...)`, the `?` unwinds `run()`, `rx` drops, and `tx.closed()` resolves in every remaining worker — all concurrent `COPY` loops abort simultaneously. This is the only operational abort path. There is no way to cancel a single table's copy without bringing down every other table in the same manager.

**3. The `tx.is_closed()` guard fires after permit acquisition, not before.**

```rust
let _permit = Arc::clone(&self.permit)
    .acquire_owned()
    .await
    .map_err(|_| Error::ParallelConnection)?;

if self.tx.is_closed() {          // ← checked here
    return Err(Error::DataSyncAborted);
}
```

Tasks that are queued behind the semaphore when the coordinator dies continue to wait for a permit. They do not observe that the coordinator is gone until after they acquire a permit. Under high concurrency this means every queued task wakes, acquires a permit, checks `is_closed()`, and immediately returns an error — burning a permit round-trip for each one.

**4. The name is a lie told to the next reader.**

A reader seeing `AbortSignal` at a call site infers that an active abort can be issued. The implementation has no such capability. The name suppresses the question "who calls abort?" and makes the passive channel-closed mechanism invisible. This is the most dangerous property: future code that tries to use `AbortSignal` to implement a real abort — a timeout, a graceful stop, a per-table cancel — will find no mechanism to do so and is likely to add a second, parallel cancellation path instead of understanding the existing one.

### Cause

The signal was built to satisfy a narrow requirement — stop in-flight copies when any copy fails — and named optimistically. The implementation accidentally works for that single case because error propagation through `?` drops `rx` as a side effect. The gap between the name and the mechanism was not visible until the wider behaviour of `rx` (its lifetime, its relationship to `tokio::spawn`, the absence of an explicit abort path) was examined.

### Code references

| Symbol | File |
---|---|
| `AbortSignal` — full definition | [`pgdog/src/backend/replication/logical/publisher/abort.rs`](../../pgdog/src/backend/replication/logical/publisher/abort.rs) |
| `ParallelSync::run_with_retry()` — constructs `AbortSignal` per attempt | [`pgdog/src/backend/replication/logical/publisher/parallel_sync.rs`](../../pgdog/src/backend/replication/logical/publisher/parallel_sync.rs) |
| `Table::data_sync()` — `select!` arm polling `abort.aborted()` | [`pgdog/src/backend/replication/logical/publisher/table.rs`](../../pgdog/src/backend/replication/logical/publisher/table.rs) |
| `ParallelSyncManager::run()` — owns `rx`; drops it on exit | [`pgdog/src/backend/replication/logical/publisher/parallel_sync.rs`](../../pgdog/src/backend/replication/logical/publisher/parallel_sync.rs) |

### Fix

Replace `AbortSignal` with a `CancellationToken` (from `tokio-util`) or a `watch::Sender<bool>` with an explicit `cancel()` method. The cancellation handle must be cloneable, sendable to workers before they start, and triggerable by an external caller — not just by the death of a channel receiver.

```rust
// tokio-util approach:
use tokio_util::sync::CancellationToken;

let cancel = CancellationToken::new();

// Pass a clone to each worker:
let worker_cancel = cancel.clone();

// Inside data_sync COPY loop:
select! {
    _ = worker_cancel.cancelled() => {
        return Err(Error::CopyAborted(self.table.clone()));
    }
    result = copy_sub.copy_data(data_row) => { ... }
}

// To stop all workers at any time:
cancel.cancel();
```

With this shape:
- The manager can cancel all workers explicitly without dying first.
- A timeout or external stop signal can call `cancel.cancel()` without needing to propagate an error through the channel.
- Per-table cancellation is possible by giving each worker its own child token: `cancel.child_token()`.
- The `tx.is_closed()` pre-flight check becomes `cancel.is_cancelled()`, which is honest about what it is testing.

The `AbortSignal` type should be deleted. It carries no state that cannot be replaced by the token directly, and its existence perpetuates the misleading name.

### References

- [`tokio_util::sync::CancellationToken`](https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html) — cooperative cancellation token with child-token support and `cancelled().await`.
- [`tokio::sync::watch`](https://docs.rs/tokio/latest/tokio/sync/watch/index.html) — persistent value channel; used for the stop-signal fix in Issue 2.