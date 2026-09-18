# Code review: replication orchestrator decomposition

Scope: `main`..`HEAD`. The change removes
`pgdog/src/backend/replication/logical/orchestrator.rs` (-762). Its logic moves
into `pgdog/src/api/replication.rs` and into three new publisher modules:
`replication_stream.rs`, `cutover_policy.rs`, `replication_progress.rs`.
`pgdog-stats/src/task.rs` splits into
`task/{copy_data,replication,reshard,schema_sync}.rs`.

Eight reviewers covered the files in parallel. Every finding cites code a
reviewer read, compared against the pre-image with `git show main:<path>`.

This file is itself a finding. See "Delete this file" below.

Status: 2 blockers and 11 other findings fixed and verified. 2 findings
withdrawn as wrong. 1 blocker, 3 majors, and the minors and nits below remain
open.

Verification after the last change: `cargo fmt --all` clean,
`cargo clippy --all-targets` clean, `cargo nextest run --profile dev` 2581
passed, 7 skipped. No integration suite was run.

## Fixed

### Lost operator `CUTOVER`

`pgdog/src/api/replication.rs:160`

`register_cutover` ran for every task, but the select arm awaits the token only
when `auto_cutover` is false. An auto task registered an entry that no code
consumed. `trigger_cutover` found it, cancelled it, and returned `true`, so
`CUTOVER` answered `OK` and did nothing. A bare `CUTOVER` picks the lowest root
id, so one `RESHARD` plus one waiting `COPY_DATA` meant the command hit the
auto task and the waiting task never cut over.

Fix: `let cutover = (!auto_cutover).then(|| Self::register_cutover(ctx.root_id()));`
The registry holds only tasks that await the token.

### Lost shutdown in the drain

`publisher/replication_stream.rs:108-120`, `publisher/slot.rs:422`

`stopping` was a write-once local latch. After the first stop the
`stop.cancelled()` arm stayed disabled and `CopyDone` was never sent again.
`try_join!` at the retry site returns on the first error and drops the other
future, and `start_replication` clears `stopped` as its first statement. So a
`stream.reconnect()` error left the slot live-streaming with `stopped == false`
and `stopping == true`. The error is retryable, so the loop continued and the
drain never ended. The cutover died on the 300 s `drain_timeout`.

Fix: `ReplicationSlot::stopped()`, read at the top of every loop pass, with
`biased` so the re-send is prompt. `CancellationToken::cancelled()` is
level-triggered, so any path that clears `stopped` re-arms the arm.

### A migration could never report success

`pgdog/src/api/task.rs:188,507`

`TaskEntry::transition` rewrote every terminal state to `Cancelled` while the
token was cancelled. `ReplicationTask` cannot return `Ok` with a live token,
because the loop has no break and the only success exit sits inside
`if is_cancelled()`. So the rewrite always fired, and `Finished` was
unreachable for `ReplicationTask` and for `ReshardTask`, which awaits it. A
finished migration and an aborted one looked the same.

Fix: the rewrite moved into the root watcher and applies to an error only. A
cooperative `Ok` after a cancel reports `Finished`, so `STOP_TASK` during a
reverse phase finishes the migration. A subtask keeps its honest `Finished` or
`Error`, because a parent cannot read a child's state and may continue after a
child fails. Three tests follow the new contract: `api/task.rs:1124`,
`api/task.rs:1190`, and `integration/.../replication.rs:219`.

### Shard tasks could be detached with no stop signal

`pgdog/src/api/replication.rs:348`

The shard streams are spawned tasks held as `JoinHandle` values, and dropping a
`JoinHandle` detaches the task rather than aborting it. `streams_stop.cancel()`
runs after the loop, so a cluster future dropped before that line left its
children with no stop signal. Those tasks replicate forever and never reach
`slot.drop_slot()`, so they hold their slots and their connections. The parent
drain timeout reaches exactly that state.

Fix: `let _streams_stop_on_drop = streams_stop.clone().drop_guard();` A detached
task always receives the stop, drains, and drops its slot.

Rejected alternative: `AbortOnDropHandle`. Aborting drops the shard future at
its next await point, so `ReplicationShardTask::run` never reaches
`slot.drop_slot()`. That converts an eventual cleanup into a guaranteed slot
leak. Cancel and drain is the only correct mechanism here.

### The cutover could leak permanent replication slots

`pgdog/src/api/replication.rs:89-101`

`Self::cutover` creates the reverse slots before the traffic switch, and they
then live only in `orchestrator.publisher`. Nothing dropped them until
`pop_slot` handed each one to a shard task. That window holds
`resume_traffic`, `orchestrator.refresh`, and the reverse `prepare_replication`,
which does network round trips. `cutover` cleaned up only when `cutover` itself
failed. `ReshardTask`'s guard holds the pre-`refresh_publisher` `Arc`, so it
cannot see the reverse slots, and standalone `REPLICATE` had no owner at all.

Fix: `run` is a thin wrapper, the phase loop moved into `migrate`, and the
wrapper cleans up on every exit.

```rust
let result = Self::migrate(&ctx, &mut orchestrator, schema_sync, auto_cutover).await;

if let Err(err) = Box::pin(orchestrator.publisher().await.cleanup()).await {
    warn!("failed to clean up replication slots: {err}");
}
```

`run` owns the orchestrator, so it reads the publisher directly. That matters:
`publication_guard` clones the `Arc`, and `refresh_publisher` installs a new one
during the cutover, so a guard is valid only for the publisher that existed when
it was taken. `Publisher::cleanup` takes the slot map with `std::mem::take`, so
it is idempotent and a no-op once replication claimed the slots. The narrower
guard inside `cutover` is now redundant and gone. `ReshardTask` keeps its
`PublicationGuard`, which covers the earlier `data_sync` window.

### `stop_replication` set its flag last

`publisher/slot.rs:414-424`

If `send_one` succeeded and `flush` failed, `stopped` stayed false while a
partial `CopyDone` was on the wire. `status_update` then wrote `CopyData` onto a
half it must treat as closed, which is a protocol violation.

Fix: set the flag first, and return early when it is already set.

```rust
if self.stopped {
    return Ok(());
}

self.stopped = true;
self.server()?.send_one(&CopyDone.into()).await?;
self.server()?.flush().await?;
```

### A failed stop request failed the whole stream

`publisher/replication_stream.rs:113-120`

`slot.stop_replication().await?` sat outside the `done` match, so it bypassed
the retry path. `CopyDone` on a connection that just died turned a requested
shutdown into a hard error, and `result.and(drained)` then aborted the cutover.

Fix: warn instead of propagate. The next read returns a retryable error, the
retry path reconnects, and `reconnect` re-sends `CopyDone`. The two fixes above
depend on each other: setting the flag first is what makes warn-and-continue
safe, because `status_update` then stops writing even when the send failed.

### The traffic-stop wait had no deadline

`publisher/cutover_policy.rs:63-110`

`wait_for_stop_threshold` looped on a one-second tick with no budget, and
`cutover_timeout` applies only to `wait_for_catchup`. A lag that never falls
below `traffic_stop_threshold`, including a frozen reading from a dead meta
connection, held the cutover open forever. Not in the original review; found
while tracing the lag.

Fix: give up after `CutoverConfig::timeout` with `Error::AbortTimeout`. Traffic
is still flowing at that point, so aborting costs nothing.

### An early return skipped the stop and the drain

`pgdog/src/api/replication.rs:171`

```rust
result = &mut cluster_run => {
    result?;
    return Err(Error::ReplicationStreamStopped);
}
```

The `return` bypassed `stop_cluster_replication.stop` and `drain_streams`. The
arm was unreachable, because the cluster task only completes `Err` here and
`result?` already carries that error. Fix: `result = &mut cluster_run => result`,
so the shared tail runs. The synthetic error construction went with it.

### Smaller fixes

- `api/replication.rs:434-437`. `drain_streams` no longer spends
  `cancel_timeout`, the task framework's abort grace period. It has
  `stream_drain_timeout()` at 120 s, beside the parent's `drain_timeout()` at
  300 s.
- `replication_progress.rs:33-44`. `updater_for_shard` asserts the index, so a
  bad shard fails at the call site instead of panicking inside a spawned task.
- `replication_stream.rs:159-166`. The commit branch built `StatusUpdate` twice
  per commit, each with a fresh `postgres_now()` clock read. It reads
  `su.last_applied` before handing `su` over.
- `replication_stream.rs:25`. The stream held a full `Cluster` clone per shard
  to use one name. It holds `source_name: String`.
- `replication_stream.rs:97`. `check_lag` uses `MissedTickBehavior::Delay`, so a
  slow retry no longer burst-fires catalog queries on resume.
- `api/replication.rs:280,293`. `ReplicationClusterStop` and
  `ReplicationClusterTask` are private. No caller outside the module.
- `cutover_policy.rs:189-196`. The wildcard `Go` arm sits with the other `Go`
  arm instead of after `NoGo`. `wait_for_catchup` no longer rebinds three config
  fields that `should_cutover` reads again.
- Four comments stated the opposite of the code. Corrected, not deleted:
  `api/replication.rs:125-127` on the point of no return,
  `api/resharding.rs:126-130` on what a stop resolves to, and the two grouping
  labels in `pgdog-stats/src/task.rs` that marked live variants as unused.
- `backend/replication/tests.rs:146-159`. Both drain sites share
  `drain_replication`, which wraps the awaits in a 60 s timeout. A regressed
  stop path now fails the run instead of hanging it, because
  `.config/nextest.toml` sets no `terminate-after`. Both sites check `drained?`
  first, so a dead shard stream reports its real error instead of a timeout.
- `integration/.../replication.rs:204`. The reverse-replication poll calls
  `fail_if_task_errored`, so a failing reverse stream fails the test.

## Withdrawn

Two findings were wrong. Recording them so the reasoning is not repeated.

### A stale lag cannot cause a lossy cutover

The original review said a stale or lag-blind decision could cut over with
unreplicated WAL. The ordering in `replicate_until_cutover` rules that out:

```rust
stop_cluster_replication.stop(cutover_reason);
let drained = safe_timeout(ReplicationClusterTask::drain_timeout(), &mut cluster_run)
    .await
    .unwrap_or(Err(Error::ReplicationTimeout));
result.and(drained)?;
```

`migrate` runs the schema sync and `Self::cutover` only after that. Each stream
drains until `slot.replicate` returns `Ok(None)`, the walsender's
`ReadyForQuery` after `CopyDone`, so every remaining byte is applied first. A
drain over 300 s returns `Error::ReplicationTimeout`, the cutover aborts, and
`MaintenanceMode` resumes traffic.

So the lag is a scheduling heuristic for when to stop traffic, not a safety
gate. A wrong reading costs availability and lengthens the drain. It cannot lose
rows. I tried clearing the lag to `None` on a failed read and reverted it: it
makes `SHOW TASKS` flap to `lag unknown` on any transient failure and buys no
safety.

### `Error::ReplicationStreamStopped` does not break a source restart

The review said the variant is missing from `is_retryable`, so a source restart
permanently fails a migration. A source restart makes `slot.replicate` return a
retryable `Net` error, not `Ok(None)`. `Ok(None)` is `'Z'` `ReadyForQuery`,
which follows a `CopyDone` we asked for. The existing reconnect path covers the
restart case, so there is nothing to fix. The contract is still undocumented,
which is a minor below.

## Blocker

### Delete this file

The repo rules forbid an unrequested document and require the removal of scratch
files during cleanup. Run `git rm CODE_REVIEW.md` before merge and move the open
items into the pull request description.

## Majors

1. **A wire break in `pgdog-stats`.** `task.rs:376` keeps the tag
   `"replication"` while `ReplicationDefinition` dropped the required field
   `reverse`. An older peer decodes the known tag and then fails with
   `missing field reverse`. `#[serde(other)]` does not rescue a known tag, so
   the whole `TaskUpdate` tree aborts rather than one entry. The sibling renames
   were done right, because a new tag degrades to `Other`.
   Fix: use a new tag, or keep `reverse` with `#[serde(default)]`. Deferred by
   the author for now.

2. **Three payloads lack the `#[serde(default)]` their siblings carry.**
   `task/replication.rs:103,106` on `Replicating` and `StoppedForCutover`, and
   the struct-level attribute on `ReplicationShardStatus` at
   `task/replication.rs:148`. `missed_rows` is new and required, so any payload
   written without it is a hard parse error for the whole `TaskStatus`.
   `SchemaSyncStatus` and `SchemaShardStatus` both carry the attribute. Same
   family as major 1.

3. **The test helper duplicates production assembly.**
   `backend/replication/tests.rs:122-144` against
   `api/replication.rs:403-432`. Both call `prepare_replication`, `pop_tables`,
   `pop_slot`, `updater_for_shard`, and the same builder. The old entry point
   `Publisher::replicate` is gone, so the duplication is forced. Production can
   gain a builder field or a pre-flight step while all five replication tests
   keep passing on the old assembly.
   Fix: extract one `pub(crate)` builder and call it from both places.

## Minors

### Cutover heuristics

- **A stale lag stops traffic early.** `replication_stream.rs:71` returns on the
  first `?`, so a failed lag query leaves the last value in place.
  `slot.server_meta` is created lazily and is never cleared on error
  (`slot.rs:120-135`), so a dead meta connection keeps failing forever and the
  displayed lag freezes. `cutover_policy.rs:87` then stops traffic on a number
  that may be much smaller than the truth, and the drain absorbs the difference.
  A staleness marker would fix it: record the reading time beside the value and
  treat an old reading as unknown.
- **`Go(LastTransaction)` ignores the lag.** `cutover_policy.rs:113`. The
  `last_transaction` clock advances only when the subscriber applies a commit, so
  a stalled subscriber makes the source look quiet and the clock ages past the
  1000 ms default while the lag is still large. The `None` case is the same at
  t=0. Byte-identical to the pre-image, so not a regression. The effect is an
  early traffic stop.

### Dead code and clean cutover

- `ee/mod.rs:12-21`. `OrchestratorState` and `orchestrator_state` have zero
  callers, hidden by `#![allow(dead_code, unused)]`. Enterprise builds lose all
  replication and cutover state reporting. `use super::*;` at line 8 is stale
  too. Needs a decision: restore the hook calls, or delete the hooks and update
  the enterprise consumer.
- `maintenance_mode.rs`. Deleting the `#[cfg(test)] is_on` helper is correct, but
  it removed the only assertions that traffic stops when the lag gate fires and
  resumes afterwards. `MaintenanceMode::drop` in `api/replication.rs` is now the
  sole guarantee that a failed cutover un-pauses the whole deployment, and it has
  no test.
- `publisher_impl.rs:18`. `Publisher::slots` was widened to `pub(crate)`, and
  every reader is in the same file. `pop_slot` is the accessor.
- `cutover_policy.rs:67`. `wait_for_stop_threshold` was infallible when it was
  first written. It now returns a real error, so this is resolved.
- `orchestrator.rs`. The module orchestrates nothing. What remains is a 106-line
  value object holding two clusters, a publication name, a publisher, and a slot
  name. The name misleads; the orchestration lives in `ReplicationTask`.

### Error vocabulary and contracts

- `error.rs:132`. `ReplicationTimeout` means both "slot read exceeded max_wait"
  (`slot.rs`) and "drain did not finish" (`api/replication.rs`). It is classified
  retryable at `error.rs:256` with a comment that is false for the drain case.
  Add a distinct `DrainTimeout`.
- `error.rs:134`. `ReplicationStreamStopped` is a deliberate `Ok` to `Err`
  change against `main`: a stream that ends without a stop signal now fails the
  task instead of reporting `Finished`. The new state is better, but the contract
  is undocumented where the old module doc used to state the opposite.

### Progress plumbing

- `replication_progress.rs:67`. `snapshot()` is not a snapshot. It calls
  `replication_lag()` and `last_transaction()` as two passes, each locking one
  shard at a time, so it can combine a lag from shard 0 at T0 with a transaction
  time from shard 2 at T2. Harmless at 1 Hz, but the name promises atomicity.
- `replication_progress.rs:85`. `update` plus four public fields is a free-form
  mutation hook, so nothing stops `applied_lsn` from moving backwards. It is
  display-only today. Named methods with a clamp would close it.
- `api/replication.rs`. `stream_status` is a pure mapping from
  `ReplicationShardProgress` and belongs beside that type as a `From` impl.
  `MaintenanceMode` is a generic RAII traffic guard with no replication content
  and belongs in `backend/maintenance_mode.rs`.
- `tables_sync.rs:54-56`. The "one entry per source shard" invariant is created
  in a shared helper but consumed only by `Publisher::pop_tables`, two modules
  away, and `post_data_sync` does not maintain it. Pick one owner.

### Stats surface

- `task/replication.rs:52,89`. `ReplicationDefinition` and
  `ReplicationClusterDefinition` render the same string for a forward stream, so
  `SHOW TASKS` shows a parent and its child with identical text.
- `task/replication.rs:118,151`. `lag_bytes` is `Option<u64>` at the cluster
  level and `Option<i64>` at the shard level. A shard row can show a negative lag
  under a zero-lag cluster row.
- `task.rs:16`. `task::replication` collides by name with the crate-root
  `replication` module, and the root `pub use task::*` makes it a glob candidate.
- `task.rs` test module. `test_unknown_inner_status_keeps_its_kind` gained no
  case for the two new kinds, and it is the test that guards the contract broken
  in major 1.
- `resharding.rs:84`. `MissedRows` landed in `resharding.rs` among schema and
  configuration types. `replication.rs` is the obvious home. It also lacks `Eq`
  and `Hash`, which forces `ReplicationShardStatus` to drop `Eq`.

### Tests

- `cutover_policy.rs:221`. `assert!(result.is_ok())` on a function whose only
  failure is the new timeout. The safety-critical direction is untested: nothing
  asserts that the wait continues while the lag is above the stop threshold.
- `cutover_policy.rs`. No test pins that the abort deadline is measured from
  entry and never restarted. Both timeout tests use `Duration::ZERO`, so they
  would pass even if `start` were reset inside the loop.
- `publisher_impl.rs:195-198`. The assertion became tautological. The wrapper
  empties `slots` via `cleanup()` on any error, so it passes whether the abort
  happened before the first slot or after ten were created and dropped.
- `integration/.../replication.rs:23`. `prepare_replication` runs `RELOAD` with
  no settle time, while the helper it replaced and `cleanup` both sleep 500 ms
  after a reload.
- `integration/.../replication.rs:170`. The 30 s poll budget equals the default
  `cutover_timeout` of 30000 ms, whose action is abort, so a real abort reads as
  a test timeout.
- `integration/.../replication.rs:40-63`. `wait_for_values` discards the rows it
  compared, so a regression reports only "timed out waiting for replicated
  values" instead of the unexpected row.
- `backend/replication/tests.rs`. The catch-up budget grew from 10 s to 20 s and
  the poll interval from 10 ms to 50 ms. With about 10 MB of new WAL this test
  trips the 15 s nextest slow warning on loaded hosts.
- No test asserts progress accounting. Both call sites bind the
  `ReplicationProgress` to `_`, so `replication_lag`, `last_transaction`, and
  `snapshot` are exercised only by the `cutover_policy.rs` unit tests.

### Scratch markers

Five `// W:` markers ship in this branch: `show_replication_slots.rs:26`,
`api/replication.rs:228`, `api/replication.rs:407`, `progress.rs:26`,
`publisher_impl.rs:123`. Two more pre-date it at `api/copy_data.rs:118,162`, and
one sits in `task/replication.rs:117`. The repo rules allow a comment only for a
non-obvious hack. Deferred by the author for now.

Answers found while reviewing, if they are kept: `progress.rs` `new_stream()` is
not dead, `replication_stream.rs:99` calls it. `api/replication.rs:228`
`refresh_publisher` is load-bearing, because `prepare_replication` creates slots
only when `slots.is_empty()`.

## Nits

- `replication_stream.rs:60,80`. `applied_lsn` is assigned, never clamped, at
  several sites, and `SHOW TASKS` publishes it.
- `replication_stream.rs:118`. The drain has no deadline of its own. A wedged
  walsender hangs it until the caller's 300 s budget expires, with no indication
  of which slot stalled.
- `replication_stream.rs:13-19`. One sibling module is imported by absolute path
  in a block that otherwise uses `super::`.
- `cutover_policy.rs:185`. The log prints `last transaction` with a space; the
  deleted local enum printed an underscore. A log filter breaks.
- `task/replication.rs:142`. `source_shard: usize` should be `u64`, matching the
  sibling and keeping the generated schema host-independent.
- `task/replication.rs:89`. `matches!(direction, Reverse)` where `direction` is
  `Copy + PartialEq`; `==` reads better.
- `task/replication.rs:116`. `pgdog_stats::ReplicationProgress` collides by name
  with the internal tracker, so the producer writes both paths in full.
- `admin/show_replication_slots.rs:63`. The `ref` change is pure style churn
  after the rest of the file was reverted.
- `api/replication.rs:405`. "and track it's status" should be "its".
- `subscriber/pipeline.rs:353-360`. Inlining `MissedRows::record(tag)` moved
  command-tag knowledge into the wire listener. One callsite today.

## Verified clean

Each check names its evidence.

- The moves are faithful. `task/reshard.rs` and `task/copy_data.rs` are
  byte-identical to the pre-image. `task/schema_sync.rs` differs by one import
  and one item reorder. The retained part of `orchestrator.rs` is byte-identical.
  `subscriber/tests.rs` changed only the constructor calls.
- Every `select!` branch future is cancellation-safe. `Server::read` retains
  partial bytes across a drop, and `SafeInterval::tick` and
  `CancellationToken::cancelled()` are both safe. No arm body is a cancellation
  point.
- LSN accounting holds. `slot.lsn` advances only from the last confirmed flush,
  and `StreamSubscriber::reconnect` keeps `committed_lsn`, so a reconnect resumes
  from the last acked position. Keepalives are answered.
- The cutover decision is total. Three boolean predicates feed one if-else
  chain, and both `CutoverTimeoutAction` variants are handled with no wildcard. A
  double cutover is impossible: the policy performs no side effect, and every
  loop arm returns or continues.
- No lock is held across an `.await`. The `parking_lot` guards are taken and
  dropped inside one statement.
- Traffic always resumes. `MaintenanceMode::drop`, the eager resume in
  `prepare_cutover`, and the framework abort all cover it.
- `RowDescription` matches the emitted rows in `show_replication_slots.rs`. All
  ten columns match the integration layout.
- The `MissedRows` relocation is semantically identical and better. Capture
  happens at commit, so a retry cannot double-count and a reconnect cannot lose
  counts.
- `StreamSubscriber::new` by value removes a per-table clone. No callsite added
  one.
- `tables_sync.rs:54-56` is necessary and downstream-neutral. It restores the
  leniency that `pop_tables` removed, and the `EmptyPublication` check runs
  before it.
- `replication_progress.rs:48` fixes a latent stall. The pre-image cast a
  negative lag to `u64` and wrapped it near `u64::MAX`, so the cutover could
  never fire.
- The `ReplicationWaiter` cutover is complete. No shim, alias, or dead re-export
  remains.
- No `unwrap`, `expect`, `panic!`, `dbg!`, or `todo!()` sits on a runtime path in
  the reviewed files.
