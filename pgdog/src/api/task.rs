use std::fmt::Display;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use dashmap::DashMap;
use derive_more::Debug;
use parking_lot::RwLock;
pub(crate) use pgdog_stats::TaskId;
use pgdog_stats::{TaskDefinition, TaskStatus};
use tokio::select;
use tokio::sync::oneshot::{self, Receiver};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, error, info, info_span, warn};

use crate::tasks;
use crate::util::safe_timeout;

/// A composable async task.
pub(crate) trait Task: std::fmt::Debug + Send + Sync + Sized {
    /// Status payload this task reports through
    /// [`set_status`](TaskContext::set_status).
    type Status: Into<TaskStatus> + Display;
    /// Value the task resolves to on success.
    type Output: Send + 'static;
    /// Error the task may fail with.
    type Error: std::error::Error + Send + 'static;

    /// Grace period for cooperative shutdown after cancellation;
    /// once it expires, the task is force-aborted. Only a root task gets a
    /// grace period: a subtask runs in its parent's frame, so it stops when it
    /// polls its own token, or when the root's frame is dropped.
    fn cancel_timeout() -> Duration {
        Duration::from_secs(5)
    }

    /// What this task is: its name and whatever structured detail it has.
    /// Read once at creation and stored immutably on the registry entry.
    ///
    /// A bare `&'static str` names the task; a payload such as
    /// [`TableCopyDefinition`](pgdog_stats::TableCopyDefinition) also carries
    /// the structured detail the admin API reports.
    fn definition(&self) -> impl Into<TaskDefinition>;

    /// Async task main execution logic
    fn run(
        self,
        ctx: TaskContext<Self>,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}

/// How far a task got through its lifecycle — a fixed, enumerable set,
/// independent of the domain-specific status the task reports for itself
/// (which is tracked separately as [`TaskStatus`]).
#[derive(Display, Debug, Clone, PartialEq, Eq)]
pub(crate) enum TaskProgress {
    #[display("started")]
    Started,
    #[display("running")]
    Running,
    #[display("finished")]
    Finished,
    /// Cancellation has been requested; the task is winding down
    /// cooperatively and has not yet reached a terminal state.
    #[display("cancelling")]
    Cancelling,
    #[display("cancelled")]
    Cancelled,
    #[display("failed: {_0}")]
    Error(String),
    #[display("panicked: {_0}")]
    Panic(String),
}

/// Snapshot of a task's current state, readable through the registry.
#[derive(Debug, Clone)]
pub(crate) struct TaskState {
    /// What the task is, from [`Task::definition`]: its name and, where it has
    /// any, the structured detail. Immutable for the life of the task, so every
    /// snapshot shares it.
    pub(crate) definition: Arc<TaskDefinition>,
    /// Where the task is in its lifecycle (carries the error/panic message
    /// for its terminal failure variants).
    pub(crate) progress: TaskProgress,
    /// Last status reported by the task itself. Preserved across terminal
    /// transitions, so a failed or cancelled task still shows what it last
    /// reported.
    pub(crate) status: TaskStatus,
    pub(crate) started_at: SystemTime,
    pub(crate) updated_at: SystemTime,
}

impl TaskState {
    /// Whether the task reached a terminal state (finished, cancelled,
    /// errored, or panicked).
    pub(crate) fn is_terminal(&self) -> bool {
        self.progress.is_terminal()
    }
}

/// Why a task did not complete.
#[derive(Debug, Display, Error)]
pub(crate) enum TaskError<E> {
    /// The task itself returned an error.
    #[display("task failed: {_0}")]
    Failed(E),
    /// The task was cancelled.
    #[display("task was cancelled")]
    Cancelled,
    /// The task panicked.
    #[display("task panicked: {_0}")]
    Panicked(#[error(ignore)] String),
    /// The task's result was never delivered: the watcher
    /// died without reporting (e.g. runtime shutdown).
    #[display("task result was never delivered")]
    Abandoned,
}

impl TaskProgress {
    /// Whether the task reached a terminal state.
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Finished | Self::Cancelled | Self::Error(_) | Self::Panic(_)
        )
    }

    /// Whether the task failed.
    fn is_error(&self) -> bool {
        matches!(self, Self::Error(_) | Self::Panic(_))
    }
}

/// Tasks of one nesting level
#[derive(Default)]
struct TasksMap {
    map: DashMap<TaskId, Arc<TaskEntry>>,
    /// Shared counter between all the tasks in the registry
    counter: Arc<AtomicU64>,
}

impl TasksMap {
    fn child(&self) -> Self {
        Self {
            map: DashMap::new(),
            counter: self.counter.clone(),
        }
    }

    fn next_id(&self) -> TaskId {
        TaskId::new(self.counter.fetch_add(1, Ordering::Relaxed))
    }

    fn insert(&self, id: TaskId, value: Arc<TaskEntry>) {
        self.map.insert(id, value);
    }
}

/// Mutable state of the async task that is updated
/// during the execution and status updates.
struct TaskEntryState {
    updated_at: SystemTime,
    /// Where the task is in its lifecycle (carries the error/panic message
    /// for its terminal failure variants).
    progress: TaskProgress,
    /// Last status reported by the task itself; kept across terminal
    /// transitions so failed/cancelled tasks retain it.
    status: TaskStatus,
}

impl TaskEntryState {
    fn new() -> Self {
        Self {
            updated_at: SystemTime::now(),
            progress: TaskProgress::Started,
            status: TaskStatus::Other,
        }
    }
}

/// Registry entry of a queued task. Holds no `T`: the reported status is a
/// concrete [`TaskStatus`], so every task type shares one entry type.
pub(crate) struct TaskEntry {
    pub(crate) started_at: SystemTime,
    /// Id of the task itself
    pub(crate) id: TaskId,
    /// Id of the task that spawned this one; `None` for a root task.
    pub(crate) parent_id: Option<TaskId>,
    /// Id of the root task (the most parent of current) or the
    /// task id itself if it has no parent.
    pub(crate) root_id: TaskId,
    /// Nesting depth.
    pub(crate) level: usize,
    /// What the task is, from [`Task::definition`]. Immutable, so it lives
    /// outside the state lock and every snapshot shares it.
    definition: Arc<TaskDefinition>,
    cancellation_token: CancellationToken,
    /// Set once the task asks for its cancellation token: only
    /// then can it react to cancellation, so only then we'll
    /// wait for the cancellation to finish. Read for root tasks only.
    cooperative: AtomicBool,
    /// Mutable state of the task
    state: RwLock<TaskEntryState>,
    /// The map of the subtasks of the current task_entry
    subtasks: Arc<TasksMap>,
    /// The tracing span associated with the task
    tracing_span: Span,
}

impl TaskEntry {
    fn cancel(&self) {
        self.cancellation_token.cancel();
    }

    /// Transition the task to the specified progress state.
    /// No-op if the task is already in terminal state.
    fn transition(&self, mut progress: TaskProgress) {
        let _enter = self.tracing_span.enter();

        let mut state = self.state.write();
        if state.progress.is_terminal() {
            return;
        }

        let panicked = matches!(progress, TaskProgress::Panic(_));
        if progress.is_terminal() && !panicked && self.cancellation_token.is_cancelled() {
            info!(
                "The task is cancelled, ignore the current progress: {progress} and set it Cancelled"
            );
            progress = TaskProgress::Cancelled;
        }

        info!("state transition to: {progress}");

        if progress.is_error() {
            error!("task failed: {progress}");
        }

        state.progress = progress;
        state.updated_at = SystemTime::now();
    }

    pub(crate) fn state(&self) -> TaskState {
        let state = self.state.read();

        TaskState {
            definition: self.definition.clone(),
            progress: state.progress.clone(),
            status: state.status.clone(),
            started_at: self.started_at,
            updated_at: state.updated_at,
        }
    }

    pub(crate) fn expired(&self, now: SystemTime, ttl: Duration) -> bool {
        let state = self.state.read();
        state.progress.is_terminal()
            && now
                .duration_since(state.updated_at)
                .is_ok_and(|age| age >= ttl)
    }
}

struct TerminalOnDrop(Arc<TaskEntry>);

impl Drop for TerminalOnDrop {
    fn drop(&mut self) {
        let progress = if std::thread::panicking() {
            TaskProgress::Panic("unwound by a panic".into())
        } else {
            TaskProgress::Cancelled
        };

        self.0.transition(progress);
    }
}

/// Context that is passed to the [Task::run].
///
/// Generic over the task so [`set_status`](Self::set_status) only accepts that
/// task's own [`Task::Status`]; the entry it points at is type-erased, so `T`
/// is carried as a phantom.
pub(crate) struct TaskContext<T: Task> {
    task: Arc<TaskEntry>,
    _task: PhantomData<fn() -> T>,
}

impl<T: Task> Clone for TaskContext<T> {
    fn clone(&self) -> Self {
        Self {
            task: self.task.clone(),
            _task: PhantomData,
        }
    }
}

/// Handle to a spawned task. Resolves, as a future, to the
/// task's result; also exposes the registry id of the task.
#[derive(Debug)]
pub(crate) struct TaskWaiter<R, E> {
    id: TaskId,
    #[debug(ignore)]
    waiter: Receiver<Result<R, TaskError<E>>>,
}

impl<R, E> TaskWaiter<R, E> {
    pub(crate) fn id(&self) -> TaskId {
        self.id
    }
}

impl<R, E> Future for TaskWaiter<R, E> {
    type Output = Result<R, TaskError<E>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().waiter).poll(cx).map(|res| {
            // A dropped sender means the watcher died without
            // reporting; surface that as a task error instead of
            // leaking the channel's RecvError.
            res.unwrap_or_else(|_| Err(TaskError::Abandoned))
        })
    }
}

/// Finished tasks stay visible in the registry for this long
/// before being pruned.
const TASK_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

/// The main storage for async tasks
pub(crate) struct TaskStorage {
    tasks: Arc<TasksMap>,
    retention: Duration,
}

impl Default for TaskStorage {
    fn default() -> Self {
        Self::new(TASK_RETENTION)
    }
}

impl<T: Task> TaskContext<T> {
    /// [`TaskEntry::transition`].
    fn transition(&self, progress: TaskProgress) {
        self.task.transition(progress);
    }

    /// Update the status the task reports for itself.
    pub(crate) fn set_status(&self, status: T::Status) {
        let _enter = self.task.tracing_span.enter();

        let mut state = self.task.state.write();
        if state.progress.is_terminal() {
            return;
        }

        info!("status transition to: {status}");

        // Don't regress a cancellation-in-progress back to Running; the task
        // may still report status while it winds down.
        if state.progress != TaskProgress::Cancelling {
            state.progress = TaskProgress::Running;
        }

        state.status = status.into();
        state.updated_at = SystemTime::now();
    }

    /// Get the task's cancellation token.
    /// If it's called the cancellation will wait for the task finishes
    /// before aborting it (only for root tasks)
    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.task.cooperative.store(true, Ordering::Relaxed);

        self.task.cancellation_token.clone()
    }

    /// Run `task` as a subtask, inside the caller's own task.
    pub(crate) fn run<'a, T1: Task + 'a>(
        &self,
        task: T1,
    ) -> impl Future<Output = Result<T1::Output, T1::Error>> + Send + use<'a, T, T1> {
        let parent = &self.task;
        let tasks = &parent.subtasks;
        let id = tasks.next_id();
        let definition = Arc::new(task.definition().into());

        let parent_name = &parent.definition;
        let parent_id = parent.id;
        info!(
            "Starting new subtask '{definition}' (id: {id}) for parent task '{parent_name}' (id: {parent_id})"
        );
        let span = info_span!(parent: &parent.tracing_span, "task", %id);

        let entry = Arc::new(TaskEntry {
            id,
            started_at: SystemTime::now(),
            definition,
            parent_id: Some(parent_id),
            root_id: parent.root_id,
            level: parent.level + 1,
            cancellation_token: parent.cancellation_token.child_token(),
            cooperative: AtomicBool::new(false),
            subtasks: Arc::new(tasks.child()),
            state: RwLock::new(TaskEntryState::new()),
            tracing_span: span.clone(),
        });
        tasks.insert(id, entry.clone());

        let terminal = TerminalOnDrop(entry.clone());

        let ctx = TaskContext::<T1> {
            task: entry,
            _task: PhantomData,
        };

        async move {
            // make sure we cancel the task if the terminal is dropped
            let _terminal = terminal;

            match task.run(ctx.clone()).instrument(span).await {
                Ok(output) => {
                    ctx.transition(TaskProgress::Finished);

                    Ok(output)
                }
                Err(err) => {
                    ctx.transition(TaskProgress::Error(err.to_string()));

                    Err(err)
                }
            }
        }
    }

    pub(crate) fn root_id(&self) -> TaskId {
        self.task.root_id
    }
}

impl TaskStorage {
    pub(crate) fn new(retention: Duration) -> Self {
        Self {
            tasks: Arc::default(),
            retention,
        }
    }

    /// Schedule the new task as a root task for execution. A root outlives the
    /// caller's frame, so it is spawned and owns everything it touches.
    pub(crate) fn run<T: Task + 'static>(&self, task: T) -> TaskWaiter<T::Output, T::Error> {
        self.prune();

        let tasks = &self.tasks;
        let id = tasks.next_id();
        let definition = Arc::new(task.definition().into());

        info!("Starting new task '{definition}' (id: {id})");
        let span = info_span!(parent: None, "task", %id);

        let subtasks = Arc::new(tasks.child());

        let entry = TaskEntry {
            id,
            started_at: SystemTime::now(),
            definition,
            parent_id: None,
            root_id: id,
            level: 0,
            cancellation_token: CancellationToken::new(),
            cooperative: AtomicBool::new(false),
            subtasks,
            state: RwLock::new(TaskEntryState::new()),
            tracing_span: span.clone(),
        };

        let entry = Arc::new(entry);
        tasks.insert(id, entry.clone());

        let ctx = TaskContext {
            task: entry.clone(),
            _task: PhantomData,
        };

        let mut handle = tasks::spawn("async task", task.run(ctx.clone()).instrument(span));
        let (sender, receiver) = oneshot::channel();

        let cancellation_token = entry.cancellation_token.clone();

        tasks::spawn("async task waiter", async move {
            let res = select! {
                _ = cancellation_token.cancelled() => {
                    ctx.transition(TaskProgress::Cancelling);
                    if ctx.task.cooperative.load(Ordering::Relaxed) {
                        match safe_timeout(T::cancel_timeout(), &mut handle).await {
                            Ok(res) => res,
                            Err(_) => {
                                handle.abort();
                                handle.await
                            }
                        }
                    } else {
                        handle.abort();
                        handle.await
                    }
                }
                res = &mut handle => {
                    res
                }
            };

            match res {
                Ok(Ok(res)) => {
                    ctx.transition(TaskProgress::Finished);
                    let _ = sender.send(Ok(res));
                }
                Ok(Err(err)) => {
                    ctx.transition(TaskProgress::Error(err.to_string()));
                    let _ = sender.send(Err(TaskError::Failed(err)));
                }
                Err(err) if err.is_cancelled() => {
                    ctx.transition(TaskProgress::Cancelled);
                    let _ = sender.send(Err(TaskError::Cancelled));
                }
                Err(err) => {
                    let panic = err.to_string();
                    error!("task panicked: {panic}");
                    ctx.transition(TaskProgress::Panic(panic.clone()));
                    let _ = sender.send(Err(TaskError::Panicked(panic)));
                }
            }
        });

        TaskWaiter {
            id,
            waiter: receiver,
        }
    }

    /// Request cancellation of a root task. The task winds down cooperatively
    /// (or is aborted after the grace period) and its whole subtree goes with it.
    /// The subtasks are not cancellable directly.
    ///
    /// # Returns
    ///
    /// Some(state) if the task cancel in progress
    /// None if the task is not found, it's a not root task, or
    /// the task is already in terminal state.
    pub(crate) fn cancel_task(&self, id: TaskId) -> Option<TaskState> {
        let entry = self.tasks.map.get(&id)?;
        let state = entry.state();

        if state.is_terminal() {
            warn!("Task: {id} is already in terminal state and cannot be cancelled");
            return None;
        }

        entry.cancel();

        Some(state)
    }

    /// Drop every root task that reached a terminal state more than
    /// `retention` ago; running tasks are never dropped.
    fn prune(&self) {
        let now = SystemTime::now();

        // process only the root level of tasks:
        // if the root task is running - some subtasks are running
        // if the root is finished then all subtasks should be finished as well.
        // We avoid cases when tasks won't await it's subtasks.
        self.tasks
            .map
            .retain(|_, entry| !entry.expired(now, self.retention));
    }

    /// Visit entries in tasks depth-first.
    /// The handler should return ControlFlow to specify
    /// if traverse of child tasks is desirable.
    ///
    /// The handler runs while the registry's shard locks are held, so it must
    /// not call back into the registry.
    pub(crate) fn try_for_each<F>(&self, mut f: F)
    where
        F: FnMut(&TaskEntry) -> ControlFlow<()>,
    {
        fn recursive<F>(map: &TasksMap, f: &mut F)
        where
            F: FnMut(&TaskEntry) -> ControlFlow<()>,
        {
            for entry in &map.map {
                if f(entry.value()).is_continue() {
                    recursive(&entry.subtasks, f);
                }
            }
        }

        self.prune();
        recursive(&self.tasks, &mut f);
    }

    /// Visit every task in the registry, at every depth.
    #[allow(dead_code)]
    pub(crate) fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&TaskEntry),
    {
        self.try_for_each(|entry| {
            f(entry);
            ControlFlow::Continue(())
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use pgdog_stats::RatioProgress;
    use std::convert::Infallible;
    use std::fmt::Debug;
    use std::sync::Arc;
    use tokio::sync::Notify;
    use tokio::task::yield_now;
    use tokio::test;
    use tokio::time::sleep;

    type State = Arc<Mutex<&'static str>>;

    fn entries(tasks: &TasksMap) -> Vec<Arc<TaskEntry>> {
        tasks
            .map
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    impl TaskStorage {
        fn entries(&self) -> Vec<Arc<TaskEntry>> {
            entries(&self.tasks)
        }

        /// Root task under `id`. Roots only, so a subtask id reports absent.
        fn task(&self, id: TaskId) -> Option<Arc<TaskEntry>> {
            self.tasks.map.get(&id).map(|entry| entry.value().clone())
        }
    }

    impl TaskEntry {
        fn subtasks(&self) -> Vec<Arc<TaskEntry>> {
            entries(&self.subtasks)
        }
    }

    /// Progress a [`TraverseRoot`] reports at step `done` of two.
    fn step(done: u64) -> RatioProgress {
        RatioProgress { done, total: 2 }
    }

    /// What a [`Mock`] does once notified.
    #[derive(Clone, Copy, Debug)]
    enum Outcome {
        /// Mark "finished" and succeed.
        Succeed,
        /// Mark "failed" and return an error.
        Fail,
        /// Panic, leaving the state at "started".
        Panic,
    }

    /// Sets "started", waits on `notify`, then resolves per `outcome`.
    #[derive(Debug)]
    struct Mock {
        state: State,
        notify: Arc<Notify>,
        outcome: Outcome,
    }

    impl Task for Mock {
        type Status = TaskStatus;
        type Output = ();
        type Error = std::io::Error;

        fn definition(&self) -> impl Into<TaskDefinition> {
            "mock"
        }

        async fn run(self, _ctx: TaskContext<Self>) -> Result<(), std::io::Error> {
            *self.state.lock() = "started";
            self.notify.notified().await;
            match self.outcome {
                Outcome::Succeed => {
                    *self.state.lock() = "finished";
                    Ok(())
                }
                Outcome::Fail => {
                    *self.state.lock() = "failed";
                    Err(std::io::Error::other("mock task failure"))
                }
                Outcome::Panic => panic!("panicking task"),
            }
        }
    }

    macro_rules! mock_successful {
        ($state:ident, $notify:ident) => {
            Mock {
                state: $state.clone(),
                notify: $notify.clone(),
                outcome: Outcome::Succeed,
            }
        };
    }

    macro_rules! mock_failing {
        ($state:ident, $notify:ident) => {
            Mock {
                state: $state.clone(),
                notify: $notify.clone(),
                outcome: Outcome::Fail,
            }
        };
    }

    macro_rules! mock_panicking {
        ($state:ident, $notify:ident) => {
            Mock {
                state: $state.clone(),
                notify: $notify.clone(),
                outcome: Outcome::Panic,
            }
        };
    }

    /// Waits on its gate, then succeeds. Never takes its cancellation
    /// token, so it is aborted immediately when cancelled as a root task.
    #[derive(Debug)]
    struct Gate {
        gate: Arc<Notify>,
    }

    impl Task for Gate {
        type Status = TaskStatus;
        type Output = ();
        type Error = Infallible;

        fn definition(&self) -> impl Into<TaskDefinition> {
            "anonymous"
        }

        async fn run(self, _ctx: TaskContext<Self>) -> Result<(), Infallible> {
            self.gate.notified().await;
            Ok(())
        }
    }

    /// Immediately succeeds.
    #[derive(Debug)]
    struct Noop;

    impl Task for Noop {
        type Status = TaskStatus;
        type Output = ();
        type Error = Infallible;

        fn definition(&self) -> impl Into<TaskDefinition> {
            "noop"
        }

        async fn run(self, _ctx: TaskContext<Self>) -> Result<(), Infallible> {
            Ok(())
        }
    }

    /// Runs a single child task of any type (spawned via `ctx.run`),
    /// propagating its error if it fails, then waits on `notify`
    /// before finishing.
    #[derive(Debug)]
    struct Inner<C: Task> {
        state: State,
        notify: Arc<Notify>,
        child: C,
    }

    impl<C: Task> Task for Inner<C> {
        type Status = TaskStatus;
        type Output = ();
        type Error = C::Error;

        fn definition(&self) -> impl Into<TaskDefinition> {
            "inner"
        }

        async fn run(self, ctx: TaskContext<Self>) -> Result<(), C::Error> {
            *self.state.lock() = "started";

            ctx.run(self.child).await?;

            self.notify.notified().await;

            *self.state.lock() = "finished";

            Ok(())
        }
    }

    /// Takes its cancellation token, then waits on `notify` or
    /// cancellation. On cancellation it winds down for `wind_down`
    /// before stopping, returning `true`: shorter than the 30s grace
    /// window delivers a graceful `Ok(true)`, longer is force-aborted
    /// when the grace period expires.
    #[derive(Debug)]
    struct Cancellable {
        state: State,
        notify: Arc<Notify>,
        wind_down: Duration,
    }

    impl Task for Cancellable {
        type Status = TaskStatus;
        type Output = bool;
        type Error = Infallible;

        fn definition(&self) -> impl Into<TaskDefinition> {
            "cancellable"
        }

        fn cancel_timeout() -> Duration {
            Duration::from_secs(30)
        }

        async fn run(self, ctx: TaskContext<Self>) -> Result<bool, Infallible> {
            *self.state.lock() = "started";

            let token = ctx.cancellation_token();
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = token.cancelled() => {
                    sleep(self.wind_down).await;
                    *self.state.lock() = "cancelled";
                    return Ok(true);
                }
            }

            *self.state.lock() = "finished";
            Ok(true)
        }
    }

    #[derive(Debug)]
    struct FailsOnCancel;

    impl Task for FailsOnCancel {
        type Status = TaskStatus;
        type Output = ();
        type Error = std::io::Error;

        fn definition(&self) -> impl Into<TaskDefinition> {
            "fails_on_cancel"
        }

        fn cancel_timeout() -> Duration {
            Duration::from_secs(30)
        }

        async fn run(self, ctx: TaskContext<Self>) -> Result<(), std::io::Error> {
            ctx.cancellation_token().cancelled().await;
            Err(std::io::Error::other("aborted"))
        }
    }

    #[derive(Debug)]
    struct PanicsOnCancel;

    impl Task for PanicsOnCancel {
        type Status = TaskStatus;
        type Output = ();
        type Error = std::io::Error;

        fn definition(&self) -> impl Into<TaskDefinition> {
            "panics_on_cancel"
        }

        fn cancel_timeout() -> Duration {
            Duration::from_secs(30)
        }

        async fn run(self, ctx: TaskContext<Self>) -> Result<(), std::io::Error> {
            ctx.cancellation_token().cancelled().await;
            panic!("panicking during wind down");
        }
    }

    /// Subtask that spawns a grandchild gate and waits for it.
    #[derive(Debug)]
    struct Sub {
        sub_gate: Arc<Notify>,
    }

    impl Task for Sub {
        type Status = TaskStatus;
        type Output = ();
        type Error = Infallible;

        fn definition(&self) -> impl Into<TaskDefinition> {
            "anonymous"
        }

        async fn run(self, ctx: TaskContext<Self>) -> Result<(), Infallible> {
            ctx.run(Gate {
                gate: self.sub_gate,
            })
            .await
            .unwrap();

            Ok(())
        }
    }

    /// Root task that reports a [`RatioProgress`], runs a subtask, then
    /// advances its progress and waits.
    #[derive(Debug)]
    struct TraverseRoot {
        sub_gate: Arc<Notify>,
        parent_gate: Arc<Notify>,
    }

    impl Task for TraverseRoot {
        type Status = RatioProgress;
        type Output = ();
        type Error = Infallible;

        fn definition(&self) -> impl Into<TaskDefinition> {
            "test_task"
        }

        async fn run(self, ctx: TaskContext<Self>) -> Result<(), Infallible> {
            ctx.set_status(step(1));

            let sub = ctx.run(Sub {
                sub_gate: self.sub_gate,
            });

            sub.await.unwrap();

            ctx.set_status(step(2));

            self.parent_gate.notified().await;

            Ok(())
        }
    }

    /// Spawned tasks are only guaranteed to be polled after the
    /// current task yields; a few rounds cover spawn chains.
    async fn settle() {
        for _ in 0..5 {
            yield_now().await;
        }
    }

    #[test]
    async fn test_single_execution() {
        let notify = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let a = mock_successful!(state_a, notify);

        let storage = TaskStorage::default();

        let task = storage.run(a);

        settle().await;

        assert_eq!(*state_a.lock(), "started");

        notify.notify_one();

        task.await.unwrap();

        assert_eq!(*state_a.lock(), "finished");
    }

    #[test]
    async fn test_multiple_execution() {
        let notify = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let state_b = Arc::new(Mutex::new("initial"));
        let state_c = Arc::new(Mutex::new("initial"));
        let a = mock_successful!(state_a, notify);
        let b = mock_successful!(state_b, notify);
        let c = mock_successful!(state_c, notify);

        let storage = TaskStorage::default();

        let task_a = storage.run(a);
        let task_b = storage.run(b);

        let info = storage.task(task_a.id()).unwrap();

        assert_eq!(storage.entries().len(), 2);

        assert!(matches!(info.state().progress, TaskProgress::Started));

        let info = storage.task(task_b.id()).unwrap();

        assert!(matches!(info.state().progress, TaskProgress::Started));

        settle().await;

        assert_eq!(*state_a.lock(), "started");
        assert_eq!(*state_b.lock(), "started");
        assert_eq!(*state_c.lock(), "initial");

        notify.notify_one();
        notify.notify_one();

        let task_c = storage.run(c);

        notify.notify_one();

        task_c.await.unwrap();

        assert_eq!(*state_a.lock(), "finished");
        assert_eq!(*state_b.lock(), "finished");
        assert_eq!(*state_c.lock(), "finished");
    }

    #[test]
    async fn test_inner_execution() {
        let child_notify = Arc::new(Notify::new());
        let notify = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let state_c = Arc::new(Mutex::new("initial"));
        let c = Inner {
            state: state_c.clone(),
            notify: notify.clone(),
            child: mock_successful!(state_a, child_notify),
        };

        let storage = TaskStorage::default();

        let task_c = storage.run(c);

        settle().await;

        assert_eq!(*state_a.lock(), "started");
        assert_eq!(*state_c.lock(), "started");

        // Release the child; the root then parks on its gate.
        child_notify.notify_waiters();
        settle().await;

        assert_eq!(*state_a.lock(), "finished");
        assert_eq!(*state_c.lock(), "started");

        // Open the gate; the root finishes.
        notify.notify_one();

        task_c.await.unwrap();

        assert_eq!(*state_c.lock(), "finished");
    }

    #[test(start_paused = true)]
    async fn test_single_cancel() {
        let notify = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let a = mock_successful!(state_a, notify);

        let storage = TaskStorage::default();

        let task = storage.run(a);

        settle().await;

        assert_eq!(*state_a.lock(), "started");

        let task_id = task.id();
        storage.cancel_task(task_id);

        let res = task.await;
        assert!(matches!(res, Err(TaskError::Cancelled)));

        assert_eq!(*state_a.lock(), "started");

        // Cancelled tasks stay visible with a terminal status.
        let entry = storage.task(task_id).unwrap();
        assert!(matches!(entry.state().progress, TaskProgress::Cancelled));
    }

    #[test(start_paused = true)]
    async fn test_graceful_exit_during_cancel_grace() {
        let notify = Arc::new(Notify::new());
        let state = Arc::new(Mutex::new("initial"));
        let a = Cancellable {
            state: state.clone(),
            notify: notify.clone(),
            wind_down: Duration::from_secs(1),
        };

        let storage = TaskStorage::default();

        let task = storage.run(a);
        let task_id = task.id();

        settle().await;

        assert_eq!(*state.lock(), "started");

        storage.cancel_task(task_id);

        // The task observed the token and finished gracefully: its
        // result must be delivered, not discarded or lost to a
        // watcher panic.
        let res = task.await;
        assert!(res.unwrap());

        assert_eq!(*state.lock(), "cancelled");

        let entry = storage.task(task_id).unwrap();
        assert!(matches!(entry.state().progress, TaskProgress::Cancelled));
    }

    #[test(start_paused = true)]
    async fn test_error_after_cancel_reports_cancelled() {
        let storage = TaskStorage::default();

        let task = storage.run(FailsOnCancel);
        let task_id = task.id();

        settle().await;
        storage.cancel_task(task_id);

        let res = task.await;
        assert!(matches!(res, Err(TaskError::Failed(_))));

        let entry = storage.task(task_id).unwrap();
        assert_eq!(entry.state().progress, TaskProgress::Cancelled);
    }

    #[test(start_paused = true)]
    async fn test_panic_after_cancel_reports_panic() {
        let storage = TaskStorage::default();

        let task = storage.run(PanicsOnCancel);
        let task_id = task.id();

        settle().await;
        storage.cancel_task(task_id);

        let res = task.await;
        assert!(matches!(res, Err(TaskError::Panicked(_))));

        let entry = storage.task(task_id).unwrap();
        assert!(matches!(entry.state().progress, TaskProgress::Panic(_)));
    }

    #[test(start_paused = true)]
    async fn test_cancelling_status_visible_during_wind_down() {
        let notify = Arc::new(Notify::new());
        let state = Arc::new(Mutex::new("initial"));
        let a = Cancellable {
            state: state.clone(),
            notify,
            wind_down: Duration::from_secs(5),
        };

        let storage = TaskStorage::default();

        let task = storage.run(a);
        let task_id = task.id();

        settle().await;
        storage.cancel_task(task_id);
        settle().await;

        // Cancellation requested; the task is still winding down (sleeping
        // `wind_down`) and must report a non-terminal `Cancelling` status.
        let entry = storage.task(task_id).unwrap();
        assert!(matches!(entry.state().progress, TaskProgress::Cancelling));
        assert!(!entry.state().is_terminal());

        // Once it finishes winding down, the status settles to terminal.
        let res = task.await;
        assert!(res.unwrap());
        let entry = storage.task(task_id).unwrap();
        assert!(matches!(entry.state().progress, TaskProgress::Cancelled));
    }

    #[test(start_paused = true)]
    async fn test_inner_cancel() {
        let notify = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let state_c = Arc::new(Mutex::new("initial"));
        let c = Inner {
            state: state_c.clone(),
            notify: notify.clone(),
            child: mock_successful!(state_a, notify),
        };

        let storage = TaskStorage::default();

        let task_c = storage.run(c);

        settle().await;

        assert_eq!(*state_a.lock(), "started");
        assert_eq!(*state_c.lock(), "started");

        let root = storage.task(task_c.id()).unwrap();
        let sub_id = root.subtasks().remove(0).id;

        assert!(storage.cancel_task(sub_id).is_none());
        assert!(storage.cancel_task(task_c.id()).is_some());

        let res = task_c.await;
        assert!(matches!(res, Err(TaskError::Cancelled)));

        assert_eq!(*state_a.lock(), "started");
        assert_eq!(*state_c.lock(), "started");
    }

    #[test]
    async fn test_single_error() {
        let notify = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let a = mock_failing!(state_a, notify);

        let storage = TaskStorage::default();

        let task = storage.run(a);

        settle().await;

        assert_eq!(*state_a.lock(), "started");

        notify.notify_one();

        let task_id = task.id();
        let res = task.await;
        assert!(matches!(res, Err(TaskError::Failed(_))));

        assert_eq!(*state_a.lock(), "failed");

        let info = storage.task(task_id).unwrap();
        assert!(matches!(info.state().progress, TaskProgress::Error(_)));
    }

    #[test]
    async fn test_inner_error() {
        let notify = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let state_c = Arc::new(Mutex::new("initial"));
        let c = Inner {
            state: state_c.clone(),
            notify: notify.clone(),
            child: mock_failing!(state_a, notify),
        };

        let storage = TaskStorage::default();

        let task_c = storage.run(c);

        settle().await;

        assert_eq!(*state_a.lock(), "started");
        assert_eq!(*state_c.lock(), "started");

        notify.notify_one();

        // The child's failure propagates out of the root task.
        let res = task_c.await;
        assert!(matches!(res, Err(TaskError::Failed(_))));

        assert_eq!(*state_a.lock(), "failed");
        assert_eq!(*state_c.lock(), "started");
    }

    #[test]
    async fn test_panic() {
        let notify = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let a = mock_panicking!(state_a, notify);

        let storage = TaskStorage::default();

        let task = storage.run(a);

        settle().await;

        assert_eq!(*state_a.lock(), "started");

        notify.notify_one();

        let task_id = task.id();
        let res = task.await;
        assert!(matches!(res, Err(TaskError::Panicked(_))));

        assert_eq!(*state_a.lock(), "started");

        let info = storage.task(task_id).unwrap();
        assert!(matches!(info.state().progress, TaskProgress::Panic(_)));
    }

    #[test]
    async fn test_panicking_subtask_is_marked_panicked() {
        let notify = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let state_c = Arc::new(Mutex::new("initial"));
        let c = Inner {
            state: state_c.clone(),
            notify: notify.clone(),
            child: mock_panicking!(state_a, notify),
        };

        let storage = TaskStorage::default();

        let task = storage.run(c);
        let id = task.id();

        settle().await;
        notify.notify_one();

        assert!(matches!(task.await, Err(TaskError::Panicked(_))));

        let root = storage.task(id).unwrap();
        assert!(matches!(root.state().progress, TaskProgress::Panic(_)));

        let subtasks = root
            .subtasks()
            .iter()
            .map(|entry| entry.state().progress)
            .collect::<Vec<_>>();

        assert_eq!(subtasks.len(), 1);
        assert!(matches!(subtasks[0], TaskProgress::Panic(_)));
    }

    #[test]
    async fn test_traverse_statuses() {
        let sub_gate = Arc::new(Notify::new());
        let parent_gate = Arc::new(Notify::new());

        let storage = TaskStorage::default();

        let other = storage.run(Gate {
            gate: parent_gate.clone(),
        });
        let task = storage.run(TraverseRoot {
            sub_gate: sub_gate.clone(),
            parent_gate: parent_gate.clone(),
        });

        let id = task.id();

        settle().await;

        // Top-level listing: the named task reports a live status.
        let roots = storage.entries();
        assert_eq!(roots.len(), 2);

        let root = storage.task(id).unwrap();
        assert_eq!(root.state().definition.name, "test_task");
        assert!(
            root.state().progress == TaskProgress::Running && root.state().status == step(1).into()
        );

        // The subtask nests under the root, and its grandchild under the
        // subtask: every level owns its own map.
        let subs = root.subtasks();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].state().definition.name, "anonymous");
        assert!(matches!(subs[0].state().progress, TaskProgress::Started));

        let grandchildren = subs[0].subtasks();
        assert_eq!(grandchildren.len(), 1);
        assert_eq!(grandchildren[0].state().definition.name, "anonymous");
        assert!(grandchildren[0].subtasks().is_empty());

        let mut visited = vec![];
        storage.for_each(|task| visited.push(task.id));

        assert_eq!(visited.len(), 4);
        for id in [other.id(), root.id, subs[0].id, grandchildren[0].id] {
            assert!(visited.contains(&id));
        }

        sub_gate.notify_one();
        settle().await;

        // Subtask finished, parent moved to the next phase.
        let root = storage.task(id).unwrap();
        assert!(
            root.state().progress == TaskProgress::Running && root.state().status == step(2).into()
        );
        for sub in root.subtasks() {
            assert!(matches!(sub.state().progress, TaskProgress::Finished));

            for grandchild in sub.subtasks() {
                assert!(matches!(
                    grandchild.state().progress,
                    TaskProgress::Finished
                ));
            }
        }

        parent_gate.notify_waiters();

        task.await.unwrap();
        other.await.unwrap();

        // Terminal status stays observable after completion, and the last
        // inner progress is preserved across the terminal transition.
        let root = storage.task(id).unwrap();
        assert!(matches!(root.state().progress, TaskProgress::Finished));
        assert_eq!(root.state().status, step(2).into());
    }

    /// Every task points at its parent and its root, only a root has no
    /// parent, and no two tasks of one registry share an id.
    #[test]
    async fn test_every_task_records_its_parent() {
        let sub_gate = Arc::new(Notify::new());
        let parent_gate = Arc::new(Notify::new());

        let storage = TaskStorage::default();
        let task = storage.run(TraverseRoot {
            sub_gate: sub_gate.clone(),
            parent_gate: parent_gate.clone(),
        });

        settle().await;

        let root = storage.task(task.id()).unwrap();
        assert!(root.parent_id.is_none());
        assert_eq!((root.root_id, root.level), (root.id, 0));

        let sub = root.subtasks().remove(0);
        assert_eq!(sub.parent_id, Some(root.id));
        assert_eq!((sub.root_id, sub.level), (root.id, 1));

        let grandchild = sub.subtasks().remove(0);
        assert_eq!(grandchild.parent_id, Some(sub.id));
        assert_eq!((grandchild.root_id, grandchild.level), (root.id, 2));

        assert_ne!(root.id, sub.id);
        assert_ne!(sub.id, grandchild.id);
        assert_ne!(root.id, grandchild.id);

        sub_gate.notify_one();
        parent_gate.notify_one();
        task.await.unwrap();
    }

    #[test]
    async fn test_prune_expired_root_tasks() {
        let notify_a = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let a = mock_successful!(state_a, notify_a);

        let sub_gate = Arc::new(Notify::new());
        let parent_gate = Arc::new(Notify::new());

        // Zero retention: terminal tasks are pruned on next access.
        let storage = TaskStorage::new(Duration::ZERO);

        let task_a = storage.run(a);
        let id_a = task_a.id();

        let root = storage.run(TraverseRoot {
            sub_gate: sub_gate.clone(),
            parent_gate: parent_gate.clone(),
        });
        let root_id = root.id();

        settle().await;

        // Both roots running; the root has its subtask registered.
        assert_eq!(storage.entries().len(), 2);
        assert_eq!(storage.task(root_id).unwrap().subtasks().len(), 1);

        // Finish the top-level task: it expired and is pruned on the next
        // prune, while the still-running root survives.
        notify_a.notify_one();
        task_a.await.unwrap();

        storage.prune();

        let roots = storage.entries();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].id, root_id);
        assert!(storage.task(id_a).is_none());

        // Let the subtasks finish; the root parks on `parent_gate`, still
        // running. Only the root level is pruned, so the finished subtasks
        // stay with their root.
        sub_gate.notify_one();
        settle().await;
        storage.prune();

        let root_entry = storage.task(root_id).unwrap();
        assert!(matches!(root_entry.state().progress, TaskProgress::Running));
        assert_eq!(root_entry.subtasks().len(), 1);

        // The root finishes and expires too: the registry empties, and the
        // subtree goes with it.
        parent_gate.notify_one();
        root.await.unwrap();

        storage.prune();
        assert!(storage.entries().is_empty());
    }

    #[test(start_paused = true)]
    async fn test_cancel_timeout_override() {
        let notify = Arc::new(Notify::new());

        let storage = TaskStorage::default();

        let task = storage.run(Cancellable {
            state: Arc::new(Mutex::new("initial")),
            notify: notify.clone(),
            wind_down: Duration::from_secs(60),
        });

        settle().await;

        let started = tokio::time::Instant::now();

        storage.cancel_task(task.id());

        let res = task.await;
        assert!(matches!(res, Err(TaskError::Cancelled)));

        // The paused clock advances exactly by the overridden
        // grace period, not the default 5s.
        assert_eq!(started.elapsed(), Duration::from_secs(30));
    }

    #[test(start_paused = true)]
    async fn test_non_cooperative_cancel_aborts_immediately() {
        let notify = Arc::new(Notify::new());

        let storage = TaskStorage::default();

        // `Gate` never takes the cancellation token: no grace period.
        let task = storage.run(Gate {
            gate: notify.clone(),
        });

        settle().await;

        let started = tokio::time::Instant::now();

        storage.cancel_task(task.id());

        let res = task.await;
        assert!(matches!(res, Err(TaskError::Cancelled)));

        // Aborted right away: the paused clock did not advance.
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[test]
    async fn global_storage_runs_and_lists() {
        let waiter = crate::api::tasks_storage().run(Noop);
        let id = waiter.id();
        waiter.await.unwrap();
        assert!(crate::api::tasks_storage().task(id).is_some());
    }

    #[test]
    async fn test_cancel_finished_task_is_not_found() {
        let notify = Arc::new(Notify::new());
        let state = Arc::new(Mutex::new("initial"));
        let a = mock_successful!(state, notify);

        let storage = TaskStorage::default();
        let task = storage.run(a);
        let id = task.id();

        notify.notify_one();
        task.await.unwrap();

        // The task is terminal (finished) but still retained for reporting:
        // cancelling it now reports not-found, so STOP_TASK won't claim it
        // stopped a completed task (nor emit a bogus cleanup warning).
        assert!(storage.task(id).is_some());
        assert!(storage.cancel_task(id).is_none());

        // An unknown id is not-found too.
        assert!(storage.cancel_task(TaskId::new(99_u64)).is_none());
    }

    #[derive(Debug)]
    struct Siblings {
        fail_gate: Arc<Notify>,
        hang_gate: Arc<Notify>,
    }

    impl Task for Siblings {
        type Status = TaskStatus;
        type Output = ();
        type Error = std::io::Error;

        fn definition(&self) -> impl Into<TaskDefinition> {
            "siblings"
        }

        async fn run(self, ctx: TaskContext<Self>) -> Result<(), std::io::Error> {
            let failing = Mock {
                state: State::default(),
                notify: self.fail_gate,
                outcome: Outcome::Fail,
            };
            let hanging = Mock {
                state: State::default(),
                notify: self.hang_gate,
                outcome: Outcome::Succeed,
            };

            futures::future::try_join(ctx.run(failing), ctx.run(hanging)).await?;

            Ok(())
        }
    }

    #[test]
    async fn test_dropped_subtask_is_marked_terminal() {
        let storage = TaskStorage::default();
        let fail_gate = Arc::new(Notify::new());
        let hang_gate = Arc::new(Notify::new());

        let task = storage.run(Siblings {
            fail_gate: fail_gate.clone(),
            hang_gate,
        });
        let id = task.id();

        let root = storage.task(id).unwrap();
        while root.subtasks().len() < 2 {
            yield_now().await;
        }

        fail_gate.notify_one();
        assert!(task.await.is_err());

        let progress = root
            .subtasks()
            .iter()
            .map(|entry| entry.state().progress)
            .collect::<Vec<_>>();

        assert_eq!(progress.len(), 2);
        assert!(progress.iter().all(|progress| progress.is_terminal()));
        assert!(progress.contains(&TaskProgress::Cancelled));
        assert!(
            progress
                .iter()
                .any(|progress| matches!(progress, TaskProgress::Error(_)))
        );
    }

    #[derive(Debug)]
    struct DropsUnpolledSubtask;

    impl Task for DropsUnpolledSubtask {
        type Status = TaskStatus;
        type Output = ();
        type Error = std::io::Error;

        fn definition(&self) -> impl Into<TaskDefinition> {
            "drops_unpolled_subtask"
        }

        async fn run(self, ctx: TaskContext<Self>) -> Result<(), std::io::Error> {
            let child = Mock {
                state: State::default(),
                notify: Arc::new(Notify::new()),
                outcome: Outcome::Succeed,
            };

            drop(ctx.run(child));

            Ok(())
        }
    }

    #[test]
    async fn test_unpolled_subtask_is_marked_terminal() {
        let storage = TaskStorage::default();

        let task = storage.run(DropsUnpolledSubtask);
        let id = task.id();
        task.await.unwrap();

        let root = storage.task(id).unwrap();
        let progress = root
            .subtasks()
            .iter()
            .map(|entry| entry.state().progress)
            .collect::<Vec<_>>();

        assert_eq!(progress.len(), 1);
        assert_eq!(progress[0], TaskProgress::Cancelled);
    }

    #[test]
    async fn test_dropped_sender_abandons_the_waiter() {
        let (sender, receiver) = oneshot::channel::<Result<(), TaskError<Infallible>>>();
        let waiter = TaskWaiter {
            id: TaskId::new(7),
            waiter: receiver,
        };

        drop(sender);

        assert_eq!(waiter.id(), TaskId::new(7));
        assert!(matches!(waiter.await, Err(TaskError::Abandoned)));
    }
}
