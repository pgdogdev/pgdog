# Async Task Framework

Long-running operations (resharding, copy, replication, schema sync) run as background *tasks* in
`crate::api` ([`pgdog/src/api/`](../pgdog/src/api/)). The admin SQL API and the `pgdog` CLI both
start the same task through the same registry; only how they consume the result differs.

## The `Task` trait

A task is any type implementing `Task` ([`api/async_task.rs`](../pgdog/src/api/async_task.rs)):

```rust
pub trait Task: Display + Debug + Send + Sync + Sized + 'static {
    type Status: Display + Send + Sync + 'static; // inner progress; Empty = none
    type Output: Send + 'static;
    type Error: std::error::Error + Send + 'static;

    fn cancel_timeout() -> Duration { Duration::from_secs(5) }

    fn run(self, ctx: AsyncTaskContext<Self>)
        -> impl Future<Output = Result<Self::Output, Self::Error>> + Send + 'static;
}
```

`run` is the whole task. Everything else — spawning, ids, status storage, cancellation,
retention — is handled by the framework around it.

## Starting a task

`crate::api::run_task(task)` ([`api/mod.rs`](../pgdog/src/api/mod.rs)) calls
`tasks_storage().run(task)`, which delegates to the private `run_task` in `async_task.rs`. That
function:

1. allocates an id from the registry (`tasks.next_id()`); a root task's `root_id` is its own id,
2. builds the `AsyncTask` entry (cancellation token, state, tracing span) and inserts it into the
   map *before* spawning,
3. spawns the task future: `tokio::spawn(task.run(ctx).instrument(span))`,
4. spawns a second watcher future that `select!`s the task handle against its cancellation token
   and records the terminal status,
5. returns an `AsyncTaskWaiter { id, waiter }`.

The id is known before `run` does any work, so the caller can address the task immediately.

```rust
pub struct AsyncTaskWaiter<R, E> {
    id: AsyncTaskId,
    waiter: Receiver<Result<R, TaskError<E>>>, // oneshot
}
```

`AsyncTaskWaiter` is a `Future`; awaiting it yields the task's `Result`. A dropped sender (watcher
gone) maps to `Err(TaskError::Abandoned)`. `.id()` returns the id without awaiting.

The registry is process-global:

```rust
static TASKS: LazyLock<AsyncTasksStorage> = LazyLock::new(AsyncTasksStorage::default);
```

So a CLI task and an admin task land in the same `AsyncTasksStorage`, both visible to `SHOW TASKS`
and cancellable by `STOP_TASK`.

## Status

Two separate axes. The lifecycle status is a fixed enum:

```rust
pub enum TaskStatus {
    Started, Running, Cancelling,        // non-terminal
    Finished, Cancelled, Error(String), Panic(String), // terminal
}
```

The domain-specific progress is `Task::Status`, reported by the task itself via
`ctx.set_status(...)` and surfaced separately as `inner_status`. `set_status` flips the lifecycle
to `Running` (but won't regress out of `Cancelling`) and stores the rendered inner status.

The registry stores both in a type-erased `TaskState` (`name`, `status`, `inner_status`,
`started_at`, `updated_at`), so `SHOW TASKS` reads it without knowing `T`.

Transitions are write-once at the terminal boundary: `transition` and `set_status` both bail early
if `state.status.is_terminal()`, so a context clone that outlives the task can't clobber a
recorded outcome. The watcher sets the terminal status based on the `select!` arm that won:

- task returned `Ok` → `Finished` (or `Cancelled` if the token was already cancelled),
- task returned `Err(e)` → `Error(e)`, waiter gets `TaskError::Failed(e)`,
- join handle cancelled → `Cancelled` / `TaskError::Cancelled`,
- join handle panicked → `Panic(msg)` / `TaskError::Panicked(msg)`.

## Cancellation

Every task holds a `CancellationToken`; a child's token is `parent.child_token()`, so cancelling a
task cancels its whole subtree.

Cooperative vs. not is decided by one call. `ctx.cancellation_token()` sets `cooperative = true` as
a side effect and hands back the token:

```rust
pub fn cancellation_token(&self) -> CancellationToken {
    self.task.cooperative.store(true, Ordering::Relaxed);
    self.task.cancellation_token.clone()
}
```

The watcher branches on that flag when the token fires:

```rust
ctx.transition(TaskStatus::Cancelling);
if cooperative {
    // grace period, then force-abort
    match timeout(T::cancel_timeout(), &mut handle).await {
        Ok(res) => res,
        Err(_) => { handle.abort(); handle.await }
    }
} else {
    handle.abort(); handle.await   // never took the token → abort now
}
```

A cooperative task typically `select!`s its work against the token and runs its own shutdown
before `run` returns, within `cancel_timeout()` (default 5s). A non-cooperative task never takes
the token and is aborted immediately — fine when dropping the future already tears the work down
(e.g. in-flight units in a `JoinSet`).

`STOP_TASK` calls `cancel_task(id)`, which returns `None` for an unknown or already-terminal id
(so callers don't claim success or emit cleanup warnings for a finished task) and otherwise calls
`entry.cancel()`.

## Composition

`ctx.run(child)` spawns a subtask:

```rust
pub fn run<T1: Task>(&self, task: T1) -> AsyncTaskWaiter<T1::Output, T1::Error> {
    run_task(Some(&self.task), &self.task.subtasks, task)
}
```

All descendants register in the *root's* `subtasks` map, so `TaskSnapshot.subtasks` is a flat list
of every descendant ordered by id (their own `subtasks` are always empty). `SHOW TASKS` renders
the root and its running children as separate rows. `STOP_TASK` only addresses roots; the token
hierarchy propagates the cancel downward.

## Retention

`AsyncTasksStorage` prunes on every `run`/`tasks`/`task` call. `prune()` drops entries whose
terminal state is older than `retention` (`TASK_RETENTION = 24h`); running tasks are never dropped.
So an id stays addressable, with its final status and last `inner_status`, for 24h after it
finishes.

## The two callers

Both go through `AsyncTaskWaiter`; the difference is what they do with it.

- **Admin** ([`pgdog/src/admin/`](../pgdog/src/admin/)): fire-and-forget. Take `.id()`, drop the
  waiter, return the id to the client. The client polls `SHOW TASKS` and runs `STOP_TASK <id>`.
- **CLI** ([`cli.rs`](../pgdog/src/cli.rs)): await the waiter in a loop. On Ctrl-C, call into the
  registry to cancel, then keep awaiting until the task winds down before exiting.

Same task, same options, same status transitions, same error path either way.
