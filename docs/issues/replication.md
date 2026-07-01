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

The destination is a second, often worse, source of the same inflation. When the
destination shards live on the **same PostgreSQL instance** as the source (a
single-instance dev/test setup, or any deployment that co-locates them), every
row pgdog copies or replicates *into* the destination is itself a write to that
instance and advances `pg_current_wal_lsn()`. So the very act of catching the
destination up pushes the instance WAL position forward, while the source slot's
`confirmed_flush_lsn` only tracks the source publication — the measured lag rises
as replication makes progress instead of falling. Every source slot on a shared
instance reads back a near-identical, ever-growing lag, because they all subtract
their publication-scoped `confirmed_flush_lsn` from the same instance-wide
`pg_current_wal_lsn()`.

### Cause

`pg_current_wal_lsn()` is instance-scoped. `confirmed_flush_lsn` is publication-scoped. Their difference is only meaningful when a single database accounts for all writes to the instance.

### Code references

| Symbol | File |
|---|---|
| `ReplicationSlot::replication_lag()` — the lag query | [`pgdog/src/backend/replication/logical/publisher/slot.rs`](../../pgdog/src/backend/replication/logical/publisher/slot.rs) |
| `ReplicationWaiter::wait_for_replication()` — the cutover gate | [`pgdog/src/backend/replication/logical/orchestrator.rs`](../../pgdog/src/backend/replication/logical/orchestrator.rs) |
| Keepalive handler / `flush_lsn` reply (proposed `data_since_keepalive` flag) | [`pgdog/src/backend/replication/logical/publisher/publisher_impl.rs`](../../pgdog/src/backend/replication/logical/publisher/publisher_impl.rs) |
### Fix

The PostgreSQL walsender sends a keepalive message after exhausting all decoded changes available for the publication. The keepalive carries `wal_end` — the server's current WAL write position. When a keepalive arrives and no xlog data was received since the previous keepalive, the slot has drained: there is nothing for the publication between `committed_lsn` and `wal_end`, so that gap consists entirely of other databases' WAL.

In this state the client can safely reply with `flush_lsn = wal_end`. PostgreSQL sets `confirmed_flush_lsn` on the slot to `wal_end`, and the lag query returns ~0.

Reporting `wal_end` is only valid when the slot is caught up. During active streaming — where the server is sending transactions and keepalives may arrive between commits — `flush_lsn` must remain at `committed_lsn`. Reporting `wal_end` prematurely would advance `confirmed_flush_lsn` past unapplied commits; if the connection dropped, the server would restart from `wal_end` and those commits would be lost.

The proposed guard: a `data_since_keepalive` flag. Set to `true` when any xlog data message arrives; cleared to `false` when a keepalive arrives. A keepalive is the catch-up signal only when this flag is `false` — meaning no data arrived between the last keepalive and this one.

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

## ✅ Issue 2 — Stop signal only unblocked one task instead of all (resolved)

> **Resolved.** The `Arc<Notify>` stop signal was replaced with a
> [`CancellationToken`](https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html)
> held by both `Publisher` and `Waiter`
> ([`publisher_impl.rs`](../../pgdog/src/backend/replication/logical/publisher/publisher_impl.rs)).
> `Waiter::stop()` calls `stop.cancel()`, which latches permanently and wakes
> every per-shard stream task — whether already parked on `stop.cancelled()` or
> polling later — so one call unblocks all N tasks. The token (not the
> `watch::Sender<bool>` proposed below) is the primitive that shipped; both
> satisfy the "persistent value, wakes every receiver" requirement. Each stream
> latches the signal with a `stopping` flag, then drains its slot to completion.

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

## ✅ Issue 4 — Divergent code paths for the same operation (resolved)

> **Resolved.** The divergent paths were consolidated onto the `crate::api`
> background-task framework. [`ReshardTask`](../../pgdog/src/api/resharding.rs)
> is the single composite flow (pre-data schema sync → data copy → post-data
> schema sync → replication), with `auto_cutover` toggling the final cutover;
> [`CopyDataTask`](../../pgdog/src/api/copy_data.rs) is the bulk data-copy leaf
> it composes; and `REPLICATE`/`CUTOVER`/`STOP_TASK` all drive one
> [`ReplicationTask`](../../pgdog/src/api/replication.rs). The old
> `backend/replication/logical/admin.rs` (`AsyncTasks`, `TaskType`) and
> `Orchestrator::replicate_and_cutover()` referenced below no longer exist.
>
> The `notify_one()` race is gone: a cutover is delivered to the specific
> running task through a latching `CancellationToken` held in a per-task
> `CUTOVERS` map keyed by task id (`ReplicationTask::cutover`), so it cannot be
> lost or leak into a later task. Natural slot drain intentionally does *not*
> auto-cut-over in the operator flow (`REPLICATE`/`copy_data`); cutover is
> explicit (operator `CUTOVER`) or automatic only under reshard's `auto_cutover`.

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
| `TaskType::Replication` `select!` / `waiter.wait()` arm (historical, line ~150) | now `ReplicationTask::run` in [`pgdog/src/api/replication.rs`](../../pgdog/src/api/replication.rs) |
| `AsyncTasks::cutover()` / `notify_one()` (historical, line ~75) | now the cutover signal in [`pgdog/src/api/async_task.rs`](../../pgdog/src/api/async_task.rs) |
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

---

## ✅ Issue 6 — `STOP_TASK` during cutover removed the task but left it running (resolved)

> **Resolved.** In the `crate::api` framework `STOP_TASK` only *requests*
> cancellation through a `CancellationToken`; it never removes the registry
> entry. The entry is dropped only after the task future actually reaches a
> terminal state, so the registry always reflects real execution state. A
> cutover that has passed its point of no return is allowed to finish rather
> than being torn down mid-traffic-swap: `ReplicationTask` overrides
> `cancel_timeout()` with a 60 s grace
> ([`replication.rs`](../../pgdog/src/api/replication.rs)) — comfortably longer
> than the committed swap (WAL drain + schema-sync DDL + reverse-replication
> setup), so `STOP_TASK` lets it run to completion instead of aborting it after
> the 5 s default. A genuinely hung swap is still reaped once the grace expires.

### Description (old implementation)

The old `AsyncTasks` registry (deleted `backend/replication/logical/admin.rs`)
tracked each task in a `DashMap<u64, TaskInfo>`, where:

```rust
struct TaskInfo {
    abort_tx: oneshot::Sender<()>, // dropped => abort_rx resolves
    cutover: Arc<Notify>,
    task_kind: TaskKind,
    started_at: SystemTime,
}
```

A replication task was driven by a detached `tokio::spawn`ed future selecting
over three arms:

```rust
spawn(async move {
    select! {
        _ = abort_rx           => { waiter.stop(); }          // STOP_TASK
        _ = cutover.notified() => { waiter.cutover().await; } // CUTOVER
        result = waiter.wait() => { /* slot drained */ }
    }
    AsyncTasks::get().tasks.remove(&id); // self-remove on exit
});
```

`STOP_TASK` called `AsyncTasks::remove(id)`, which did
`tasks.remove(&id)` — synchronously deleting the map entry (and dropping
`abort_tx`). Two things made the cutover-then-stop sequence broken:

1. **Removal was immediate and unconditional.** The entry vanished from the map
   the instant `remove` was called, and `remove` returned the `TaskKind` as if
   it had succeeded. Nothing tied the entry's presence to the task having
   actually stopped — the map said "gone" while the spawned future was still
   running. There was no terminal state retained; `SHOW TASKS` simply stopped
   listing it.

2. **The abort arm could not win once cutover had started.** Dropping
   `abort_tx` resolves `abort_rx`, but `select!` had already committed to the
   `cutover.notified()` arm, so `waiter.cutover().await` ran to completion
   regardless. Unlike the `SchemaSync`/`CopyData` variants — which held an
   `abort_handle` and called `abort_handle.abort()` — the replication variant
   had no hard-abort path at all; `STOP_TASK` could only ever request a
   graceful `waiter.stop()`, and only if it won the race.

So after `CUTOVER` then `STOP_TASK`, the registry reported the task removed and
stopped, while the detached future kept flipping traffic, running post-data
schema sync, and setting up reverse replication — mutating cluster state with
no visibility and no way to stop it.

### How the new implementation works

The registry entry's lifetime is tied to the task future through the supervisor
in `run_task` ([`async_task.rs`](../../pgdog/src/api/async_task.rs)), not to
admin-side bookkeeping:

1. **`STOP_TASK` requests; it does not remove.**
   `AsyncTasksStorage::cancel_task` only calls `entry.cancel()` on the task's
   `CancellationToken` and leaves the entry in the map. The entry transitions
   to a terminal status (`Cancelled`/`Finished`) only when the spawned future
   returns; `prune()` then drops it after the 24h retention. A running task is
   never removed, so `SHOW TASKS` keeps showing it as `Stopping`/`CuttingOver`
   until it has genuinely finished — the map always reflects real state.

2. **A started cutover runs to completion.** `ReplicationTask::run`
   ([`replication.rs`](../../pgdog/src/api/replication.rs)) `select!`s the
   cutover arm (`waiter.cutover().await`) against `token.cancelled()`. Once the
   `CUTOVER` signal fires and that arm is chosen, `select!` is committed to
   awaiting it; a later `STOP_TASK` cancels the token but no longer has an arm
   to win. The supervisor sees the cancelled token and — because
   `ReplicationTask` is cooperative (it took its token) — waits
   `cancel_timeout()` before aborting. `ReplicationTask` overrides
   `cancel_timeout()` to **60 s** (vs. the 5 s trait default in
   [`async_task.rs`](../../pgdog/src/api/async_task.rs)), comfortably longer than
   the committed swap, so the traffic switch finishes instead of being aborted
   mid-flight. Only a swap that hangs past the grace is force-aborted.

3. **The reverse order winds down without cutover.** If `STOP_TASK` lands
   first, the `token.cancelled()` arm sets status `Stopping`, calls
   `waiter.stop()` (graceful slot drain), and returns — no traffic switch. A
   subsequent `CUTOVER` finds the `CutoverWaiter` already dropped from the
   `CUTOVERS` map, so `ReplicationTask::cutover` returns `false` and the admin
   command rejects with `NotReplication`.

Net: `STOP_TASK` gracefully stops a task that has not yet cut over, and is a
deliberate no-op against an in-flight cutover — which completes within its 60 s
grace — and in both cases the task stays visible in the registry until it has
actually stopped.

### Code references

| Symbol | File |
|---|---|
| `AsyncTasksStorage::cancel_task` — cancels the token, keeps the entry | [`pgdog/src/api/async_task.rs`](../../pgdog/src/api/async_task.rs) |
| `AsyncTasksStorage::prune` — drops only terminal entries past retention | same file |
| `run_task` supervisor — cooperative grace via `cancel_timeout`; sets terminal status on completion | same file |
| `ReplicationTask::run` / `cancel_timeout` — cutover-vs-stop `select!` | [`pgdog/src/api/replication.rs`](../../pgdog/src/api/replication.rs) |

---

## ✅ Issue 7 — Cutover swapped the cluster name but not `database_name`, repointing entries at a nonexistent database (resolved)

> **Resolved.** `Config::cutover` now pins the effective `database_name` on the
> two clusters being swapped *before* exchanging their logical `name`, so the
> name swap only changes routing identity and never the physical database an
> entry connects to.

### Description

Traffic cutover swaps the *logical* `name` of the source and destination
clusters (`Config::cutover`, `pgdog-config/src/core.rs`) — clients keep
connecting to the same name, which now resolves to the other cluster's backends.

The physical Postgres database an entry connects to, however, is resolved as
`database_name` **if set, otherwise the cluster `name`** (`Address::from`,
`pgdog/src/backend/pool/address.rs`). `Config::cutover` rewrote `name` but left
`database_name` untouched. So any database entry that relied on the default
(no explicit `database_name`, i.e. cluster name == physical DB name) was, after
the swap, silently repointed at a physical database named after the *other*
cluster — which usually does not exist.

Two symptoms, one cause:

- The pool monitor logs `FATAL: 3D000 database "<name>" does not exist` and
  retries forever against the dead config.
- The post-cutover schema restore (`schema_sync_post_cutover` →
  `PgDumpOutput::restore`) checks out a connection from that pool; since it can
  never connect, the checkout waits out `checkout_timeout` and the cutover task
  fails with `schema: checkout timeout`, past the point of no return.

Configs that set `database_name` explicitly on every entry (e.g. the
`integration/resharding` suite, where all entries use `database_name = "postgres"`)
were unaffected, which is why this stayed hidden — the swap is a no-op for the
physical target when `database_name` is already pinned.

### Cause

`database_name` defaults to the cluster `name` at connection time, and
`Config::cutover` changed `name` without first materializing that default. The
two fields are only equivalent until the name is swapped.

### Code references

| Symbol | File |
|---|---|
| `Config::cutover` — swaps `name`; now pins `database_name` first | [`pgdog-config/src/core.rs`](../../pgdog-config/src/core.rs) |
| `Address::from` — `database_name` falls back to cluster `name` | [`pgdog/src/backend/pool/address.rs`](../../pgdog/src/backend/pool/address.rs) |
| `databases::cutover` — applies the config swap and relaunches pools | [`pgdog/src/backend/databases.rs`](../../pgdog/src/backend/databases.rs) |
| `test_cutover_preserves_physical_database_name` — regression test | [`pgdog-config/src/core.rs`](../../pgdog-config/src/core.rs) |

### Fix

Before the name swap, set `database_name` to the current `name` on any entry of
the two clusters where it is unset:

```rust
for db in self.databases.iter_mut() {
    if (db.name == source || db.name == destination) && db.database_name.is_none() {
        db.database_name = Some(db.name.clone());
    }
}
```

The swap then moves only the logical name; the physical connection target is
preserved. Covered by `test_cutover_preserves_physical_database_name` (asymmetric:
one cluster relies on the default, the other sets `database_name` explicitly).
