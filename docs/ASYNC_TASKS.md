# Async Task Framework

Long-running operations (resharding, copy, replication, schema sync) run as background *tasks* in
`crate::api` ([`pgdog/src/api/`](../pgdog/src/api/)). The admin SQL API and the `pgdog` CLI both
start the same task through the same registry; only how they consume the result differs.

## The `Task` trait

A task is any type implementing `Task` ([`api/task.rs`](../pgdog/src/api/task.rs)).
The trait fixes the task's status payload `Status` (anything convertible into
`pgdog_stats::TaskStatus`), its `Output` and `Error`, an optional `cancel_timeout()`, a required
`definition()` describing what the task is, and `run`.

`definition()` returns anything convertible into a `TaskDefinition`: a bare `&'static str` names
the task, while a payload such as `TableCopyDefinition` also carries the structured detail the
admin API reports. There is no default — a task states what it is rather than inheriting a name
from a `Display` impl.

`run` is the whole task. Everything else — scheduling, ids, status storage, cancellation,
retention — is handled by the framework around it.

## Starting a task

`crate::api::run_task(task)` ([`api/mod.rs`](../pgdog/src/api/mod.rs)) calls
`tasks_storage().run(task)`, that is `TaskStorage::run` in `task.rs`. That method:

1. allocates an id from the registry's counter (`tasks.next_id()`), which every nesting level of
   that registry shares, so an id identifies one task at any depth; the task's `root_id` is its
   own id when it is a root, and the parent's `root_id` otherwise,
2. builds the `TaskEntry` entry (cancellation token, state, definition, tracing span) and inserts it
   into the map *before* spawning,
3. spawns the task future, instrumented with the task's tracing span,
4. spawns a second watcher future that `select!`s the task handle against its cancellation token
   and records the terminal status,
5. returns a `TaskWaiter`.

The id is known before `run` does any work, so the caller can address the task immediately.

`TaskWaiter` is a `Future` over a oneshot; awaiting it yields the task's `Result`. A dropped
sender (watcher gone) maps to `Err(TaskError::Abandoned)`. `.id()` returns the id without awaiting.

The registry is a process-global `TaskStorage` (`api::tasks_storage()`), so a CLI task and an
admin task land in the same one, both visible to `SHOW TASKS` and cancellable by `STOP_TASK`.

## Status

Two separate axes, named apart. **Progress** is how far the task got through its lifecycle: the
fixed `TaskProgress` enum in `task.rs`, where `Started`, `Running` and `Cancelling` are
non-terminal and `Finished`, `Cancelled`, `Error(msg)` and `Panic(msg)` are terminal.

**Status** is what the task reports about itself: `Task::Status`, set via `ctx.set_status(...)`
and stored as a `pgdog_stats::TaskStatus`
([`pgdog-stats/src/task.rs`](../pgdog-stats/src/task.rs)). `TaskContext<T>` is
generic over the task purely so `set_status` rejects another task's payload — the registry entry
it points at is a concrete, non-generic `TaskEntry`, so `T` rides along as a `PhantomData`.
`set_status` also moves
progress to `Running` (but won't regress out of `Cancelling`).

The entry keeps both, and hands them out as one `TaskState` snapshot (`definition`, `progress`,
`status`, `started_at`, `updated_at`). `definition` is an `Arc<TaskDefinition>`, read once from
`Task::definition()` at creation and shared by every snapshot: it carries the task's name and,
where the task has structured detail, its `kind` and fields. So `SHOW TASKS` reads everything
without knowing `T`, and reading a name costs no allocation.

Transitions are write-once at the terminal boundary: `transition` and `set_status` both bail early
if progress is already terminal, so a context clone that outlives the task can't clobber a
recorded outcome. The watcher sets the terminal progress based on the `select!` arm that won:

- task returned `Ok` → `Finished` (or `Cancelled` if the token was already cancelled),
- task returned `Err(e)` → `Error(e)`, waiter gets `TaskError::Failed(e)`,
- join handle cancelled → `Cancelled` / `TaskError::Cancelled`,
- join handle panicked → `Panic(msg)` / `TaskError::Panicked(msg)`.

## Cancellation

Every task holds a `CancellationToken`; a child's token is `parent.child_token()`, so cancelling a
task cancels its whole subtree.

Cooperative vs. not is decided by one call: `ctx.cancellation_token()` sets `cooperative = true` as
a side effect and hands back the token. The watcher branches on that flag when the token fires. A
cooperative task typically `select!`s its work against the token and runs its own shutdown before
`run` returns, within `cancel_timeout()` (default 5s), after which it is force-aborted. A task that
never took its token is aborted immediately — fine when dropping the future already tears the work
down (e.g. in-flight units in a `JoinSet`).

Only a root task has a watcher, so only a root task gets the grace period and the force-abort. A
subtask is cancelled through the token hierarchy and unwinds inside its parent's frame, so its own
`cancel_timeout()` is never read.

`STOP_TASK` calls `cancel_task(id)`, which addresses **root tasks only**: a subtask id reports
not-found, and the subtree is cancelled through the token hierarchy instead. It also returns
`None` for an unknown or already-terminal id, so callers don't claim success or emit cleanup
warnings for a finished task.

## Composition

`ctx.run(child)` builds a subtask and returns its future; the parent awaits it, so a subtask runs
inside the parent's own frame rather than in a spawned task. The child holds
`parent.child_token()`. The registry entry, id included, is inserted when `ctx.run` is called, so
the subtask is addressable before its future is first polled. Dropping that future marks the entry
terminal: `Cancelled` normally, or `Panic` when the frame is unwinding, so a panic inside a subtask
is not reported as a cancellation. Only the root carries the panic message, read from its
`JoinError`.

Every entry owns the map of its own direct children, so the registry is a tree: a subtask registers
into the map of the task that spawned it, at any depth. A running task reaches its own child map
through its `TaskEntry`.

`TaskStorage::try_for_each` walks that tree depth-first, visiting a task before its subtasks.
Levels come out in unspecified order. The handler runs while the registry's shard locks are held,
so it must not call back into the registry. It prunes rather than short-circuits: `Break` skips
that entry's subtasks and the walk continues with its siblings. `for_each` is the same walk with
no pruning.

`SHOW TASKS` uses the pruning to bound depth at `MAX_LEVEL = 1`: roots and their direct children
only. Nothing goes deeper today — `ReshardTask` is the only task that starts subtasks, and its four
children (pre- and post-data schema sync, copy data, replication) start none of their own. The
bound therefore guards a deeper tree, where one row per copied table or per replication slot would
make a single reshard emit tens of thousands of rows.

Every entry carries its `level`. `SHOW TASKS` reports it through two columns that never overlap:
`id` holds the id `STOP_TASK` accepts, so it is filled for a root and `NULL` for a subtask, while
`parent_id` is `NULL` for a root and the spawning task's id for a subtask. A subtask therefore
shows the root it belongs to without offering an id that `cancel_task` would reject.

## Retention

`TaskStorage` prunes on every `run` and `try_for_each` call. `prune()` drops entries whose
terminal state is older than `retention` (`TASK_RETENTION = 24h`); running tasks are never dropped.
So an id stays addressable, with its final lifecycle and last reported status, for 24h after it
finishes.

## The two callers

Both go through `TaskWaiter`; the difference is what they do with it.

- **Admin** ([`pgdog/src/admin/`](../pgdog/src/admin/)): fire-and-forget. Take `.id()`, drop the
  waiter, return the id to the client. The client polls `SHOW TASKS` and runs `STOP_TASK <id>`.
- **CLI** ([`cli.rs`](../pgdog/src/cli.rs)): await the waiter in a loop. On Ctrl-C, call into the
  registry to cancel, then keep awaiting until the task winds down before exiting.

Same task, same options, same status transitions, same error path either way.
