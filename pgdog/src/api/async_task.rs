use std::fmt::Debug;
use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use dashmap::DashMap;
use parking_lot::RwLock;
use pgdog_postgres_types::ToDataRowColumn;
use tokio::select;
use tokio::sync::oneshot::{self, Receiver};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Represent the ID of the async task.
#[derive(Copy, Clone, Debug, Display, FromStr, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct AsyncTaskId(u64);

impl ToDataRowColumn for AsyncTaskId {
    fn to_data_row_column(&self) -> pgdog_postgres_types::Data {
        self.0.to_data_row_column()
    }
}

#[cfg(test)]
impl From<u64> for AsyncTaskId {
    fn from(value: u64) -> Self {
        AsyncTaskId(value)
    }
}

/// Status type for tasks that report no intermediate progress.
pub type Empty = std::convert::Infallible;

/// A composable async task.
pub trait Task: Display + Debug + Send + Sync + Sized + 'static {
    /// Progress-status payload reported through
    /// [`set_status`](AsyncTaskContext::set_status) while the task
    /// runs; the [`Empty`] type reports no intermediate progress.
    type Status: Display + Send + Sync + 'static;
    /// Value the task resolves to on success.
    type Output: Send + 'static;
    /// Error the task may fail with.
    type Error: std::error::Error + Send + 'static;

    /// Grace period for cooperative shutdown after cancellation;
    /// once it expires, the task is force-aborted.
    fn cancel_timeout() -> Duration {
        Duration::from_secs(5)
    }

    /// Async task main execution logic
    fn run(
        self,
        ctx: AsyncTaskContext<Self>,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send + 'static;
}

/// Predefined lifecycle status of a task — a fixed, enumerable set,
/// independent of the task's domain-specific progress (which is tracked
/// separately as the inner status).
#[derive(Display, Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
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

/// Type-erased snapshot of a task's current state,
/// readable through the registry without knowing `T`.
#[derive(Debug, Clone)]
pub struct TaskState {
    pub name: String,
    /// Predefined lifecycle status (carries the error/panic message
    /// for its terminal failure variants).
    pub status: TaskStatus,
    /// Last inner progress reported by the task, rendered to a string.
    /// Preserved across terminal transitions, so a failed or cancelled
    /// task still shows its last known progress.
    pub inner_status: Option<String>,
    pub started_at: SystemTime,
    pub updated_at: SystemTime,
}

impl TaskState {
    /// Whether the task reached a terminal state (finished, cancelled,
    /// errored, or panicked) and is only retained for status reporting.
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }
}

/// Why a task did not complete, delivered to the waiter
/// as the error half of its `Result`.
#[derive(Debug, Display, Error)]
pub enum TaskError<E> {
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

impl TaskStatus {
    /// Whether the task reached a terminal state.
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Finished | Self::Cancelled | Self::Error(_) | Self::Panic(_)
        )
    }
}

/// Represent the storage of tasks based on it's id
#[derive(Default)]
struct TasksMap {
    map: DashMap<AsyncTaskId, Arc<dyn TaskMapEntry>>,
    counter: AtomicU64,
}

impl TasksMap {
    fn next_id(&self) -> AsyncTaskId {
        AsyncTaskId(self.counter.fetch_add(1, Ordering::Relaxed))
    }

    fn insert(&self, id: AsyncTaskId, value: Arc<dyn TaskMapEntry>) {
        self.map.insert(id, value);
    }
}

/// Mutable state of the async task that is updated
/// during the execution and status updates.
struct AsyncTaskState<S = Empty> {
    updated_at: SystemTime,
    /// Predefined lifecycle status (carries the error/panic message for
    /// its terminal failure variants).
    status: TaskStatus,
    /// Last inner progress reported by the task; kept across terminal
    /// transitions so failed/cancelled tasks retain their last progress.
    inner_status: Option<S>,
}

impl<S> AsyncTaskState<S> {
    fn new() -> Self {
        Self {
            updated_at: SystemTime::now(),
            status: TaskStatus::Started,
            inner_status: None,
        }
    }
}

/// Represent the info about queued task
struct AsyncTask<T: Task> {
    started_at: SystemTime,
    /// Id of the root task this task belongs to — its own id when it is a
    /// root, inherited from the parent otherwise.
    root_id: AsyncTaskId,
    /// Name of task based on [Task] Display implementation
    name: String,
    cancellation_token: CancellationToken,
    /// Set once the task asks for its cancellation token: only
    /// then can it react to cancellation, so only then we'll
    /// wait for the cancellation to finish.
    cooperative: AtomicBool,
    /// Mutable state of the task
    state: Arc<RwLock<AsyncTaskState<T::Status>>>,
    /// The reference to the root map of tasks
    subtasks: Arc<TasksMap>,
}

/// Wrapper trait for [AsyncTask] that is not tied to specific
/// type T that allows to store tasks of different types inside.
trait TaskMapEntry: Send + Sync + 'static {
    fn cancel(&self);
    fn state(&self) -> TaskState;
    fn subtasks(&self) -> &TasksMap;
    fn expired(&self, now: SystemTime, ttl: Duration) -> bool;
}

impl<T: Task> TaskMapEntry for AsyncTask<T> {
    fn cancel(&self) {
        self.cancellation_token.cancel();
    }

    fn state(&self) -> TaskState {
        let state = self.state.read();

        TaskState {
            name: self.name.clone(),
            status: state.status.clone(),
            inner_status: state.inner_status.as_ref().map(ToString::to_string),
            started_at: self.started_at,
            updated_at: state.updated_at,
        }
    }

    fn subtasks(&self) -> &TasksMap {
        &self.subtasks
    }

    fn expired(&self, now: SystemTime, ttl: Duration) -> bool {
        let state = self.state.read();
        state.status.is_terminal()
            && now
                .duration_since(state.updated_at)
                .is_ok_and(|age| age >= ttl)
    }
}

/// Context that is passed to the [Task::run]
pub struct AsyncTaskContext<T: Task> {
    task: Arc<AsyncTask<T>>,
}

impl<T: Task> Clone for AsyncTaskContext<T> {
    fn clone(&self) -> Self {
        Self {
            task: self.task.clone(),
        }
    }
}

/// Handle to a spawned task. Resolves, as a future, to the
/// task's result; also exposes the registry id of the task.
#[derive(derive_more::Debug)]
pub struct AsyncTaskWaiter<R, E> {
    id: AsyncTaskId,
    #[debug(ignore)]
    waiter: Receiver<Result<R, TaskError<E>>>,
}

impl<R, E> AsyncTaskWaiter<R, E> {
    pub fn id(&self) -> AsyncTaskId {
        self.id
    }
}

impl<R, E> Future for AsyncTaskWaiter<R, E> {
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

/// Point-in-time view of a root task and its subtasks.
///
/// All descendants register with the root task, so `subtasks` is a
/// flat list of every descendant (children and grandchildren alike),
/// ordered by id; their own `subtasks` are always empty.
#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    pub id: AsyncTaskId,
    pub state: TaskState,
    pub subtasks: Vec<TaskSnapshot>,
}

impl TaskSnapshot {
    fn new(id: AsyncTaskId, entry: &dyn TaskMapEntry) -> Self {
        let mut subtasks: Vec<_> = entry
            .subtasks()
            .map
            .iter()
            .map(|sub| TaskSnapshot {
                id: *sub.key(),
                state: sub.value().state(),
                subtasks: Vec::new(),
            })
            .collect();
        subtasks.sort_unstable_by_key(|task| task.id);

        Self {
            id,
            state: entry.state(),
            subtasks,
        }
    }
}

/// Finished tasks stay visible in the registry for this long
/// before being pruned.
const TASK_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

/// The main storage for async tasks
pub struct AsyncTasksStorage {
    tasks: Arc<TasksMap>,
    retention: Duration,
}

impl Default for AsyncTasksStorage {
    fn default() -> Self {
        Self::new(TASK_RETENTION)
    }
}

fn run_task<P: Task, T: Task>(
    parent_task: Option<&AsyncTask<P>>,
    tasks: &TasksMap,
    task: T,
) -> AsyncTaskWaiter<T::Output, T::Error> {
    // Allocate the id up front so a root task can record its own id as its
    // root id; descendants inherit the root's id from their parent.
    let id = tasks.next_id();
    let root_id = parent_task.map(|p| p.root_id).unwrap_or(id);

    let state = Arc::new(RwLock::new(AsyncTaskState::new()));

    let entry = AsyncTask {
        started_at: SystemTime::now(),
        name: task.to_string(),
        root_id,
        cancellation_token: match parent_task {
            Some(parent) => parent.cancellation_token.child_token(),
            None => CancellationToken::new(),
        },
        cooperative: AtomicBool::new(false),
        // Descendants share the root task's registry: every descendant
        // registers as a direct child of the root.
        subtasks: if let Some(parent) = parent_task {
            parent.subtasks.clone()
        } else {
            Arc::new(TasksMap::default())
        },
        state: state.clone(),
    };

    let entry = Arc::new(entry);
    // Make sure we insert task to map before it's actually started.
    tasks.insert(id, entry.clone());

    let ctx = AsyncTaskContext {
        task: entry.clone(),
    };

    let mut handle = tokio::spawn(task.run(ctx.clone()));
    let (sender, receiver) = oneshot::channel();

    let cancellation_token = entry.cancellation_token.clone();

    tokio::spawn(async move {
        let res = select! {
            _ = cancellation_token.cancelled() => {
                ctx.transition(TaskStatus::Cancelling);
                if ctx.task.cooperative.load(Ordering::Relaxed) {
                    match timeout(T::cancel_timeout(), &mut handle).await {
                        Ok(res) => res,
                        Err(_) => {
                            // The timeout fired: abort the task
                            handle.abort();
                            handle.await
                        }
                    }
                } else {
                    // The task never took its cancellation token, so it
                    // cannot react to it: abort immediately.
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
                let status = if cancellation_token.is_cancelled() {
                    TaskStatus::Cancelled
                } else {
                    TaskStatus::Finished
                };
                ctx.transition(status);
                let _ = sender.send(Ok(res));
            }
            Ok(Err(err)) => {
                ctx.transition(TaskStatus::Error(err.to_string()));
                let _ = sender.send(Err(TaskError::Failed(err)));
            }
            Err(err) if err.is_cancelled() => {
                ctx.transition(TaskStatus::Cancelled);
                let _ = sender.send(Err(TaskError::Cancelled));
            }
            Err(err) => {
                let panic = err.to_string();
                ctx.transition(TaskStatus::Panic(panic.clone()));
                let _ = sender.send(Err(TaskError::Panicked(panic)));
            }
        }
    });

    AsyncTaskWaiter {
        id,
        waiter: receiver,
    }
}

impl<T: Task> AsyncTaskContext<T> {
    /// Move the task to a new lifecycle `status` (terminal or the
    /// non-terminal `Cancelling`), preserving the last inner progress.
    /// No-op once the task is already terminal.
    fn transition(&self, status: TaskStatus) {
        let mut state = self.task.state.write();
        if state.status.is_terminal() {
            return;
        }
        state.status = status;
        state.updated_at = SystemTime::now();
    }

    /// Update the inner progress status of the current task.
    pub fn set_status(&self, status: T::Status) {
        let mut state = self.task.state.write();
        if state.status.is_terminal() {
            return;
        }
        // Don't regress a cancellation-in-progress back to Running; the task
        // may still report inner progress while it winds down.
        if state.status != TaskStatus::Cancelling {
            state.status = TaskStatus::Running;
        }
        state.inner_status = Some(status);
        state.updated_at = SystemTime::now();
    }

    /// Hand out this task's cancellation token.
    /// Tasks that never take it are aborted immediately.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.task.cooperative.store(true, Ordering::Relaxed);

        self.task.cancellation_token.clone()
    }

    /// Run the new task as a subtask of the current one
    pub fn run<T1: Task>(&self, task: T1) -> AsyncTaskWaiter<T1::Output, T1::Error> {
        run_task(Some(&self.task), &self.task.subtasks, task)
    }

    /// Id of the root task this task belongs to (its own id when it is a
    /// root). Used to address the task's cutover signal.
    pub fn root_id(&self) -> AsyncTaskId {
        self.task.root_id
    }
}

impl AsyncTasksStorage {
    pub fn new(retention: Duration) -> Self {
        Self {
            tasks: Arc::default(),
            retention,
        }
    }

    /// Schedule the new task as a root task for execution
    pub fn run<T: Task>(&self, task: T) -> AsyncTaskWaiter<T::Output, T::Error> {
        self.prune();

        run_task::<T, T>(None, &self.tasks, task)
    }

    /// Request cancellation of a task. The task winds down
    /// cooperatively (or is aborted after the grace period) and
    /// stays in the registry with a terminal status until pruned.
    /// Returns the state the task was in when cancellation was requested,
    /// or `None` for an unknown or already-terminal id.
    pub fn cancel_task(&self, id: AsyncTaskId) -> Option<TaskState> {
        let entry = self.tasks.map.get(&id)?;
        let state = entry.state();

        // Already terminal (finished/cancelled/errored) but still retained for
        // status reporting: nothing to cancel. Report as not found so callers
        // don't claim success or emit cleanup warnings for a finished task.
        if state.is_terminal() {
            warn!("Task: {id} is already in terminal state and cannot be cancelled");
            return None;
        }

        entry.cancel();

        Some(state)
    }

    /// Drop every task that reached a terminal state more than
    /// `retention` ago; running tasks are never dropped.
    fn prune(&self) {
        let now = SystemTime::now();

        self.tasks.map.retain(|_, entry| {
            entry
                .subtasks()
                .map
                .retain(|_, sub| !sub.expired(now, self.retention));

            !entry.expired(now, self.retention)
        });
    }

    /// Snapshot every root task with its subtasks, ordered by id.
    pub fn tasks(&self) -> Vec<TaskSnapshot> {
        self.prune();

        let mut tasks: Vec<_> = self
            .tasks
            .map
            .iter()
            .map(|entry| TaskSnapshot::new(*entry.key(), entry.value().as_ref()))
            .collect();
        tasks.sort_unstable_by_key(|task| task.id);
        tasks
    }

    /// Snapshot a single root task by id.
    pub fn task(&self, id: AsyncTaskId) -> Option<TaskSnapshot> {
        self.prune();

        self.tasks
            .map
            .get(&id)
            .map(|entry| TaskSnapshot::new(id, entry.value().as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::convert::Infallible;
    use std::fmt::Debug;
    use std::sync::Arc;
    use tokio::sync::Notify;
    use tokio::task::yield_now;
    use tokio::test;
    use tokio::time::sleep;

    type State = Arc<Mutex<&'static str>>;

    #[derive(Display, Debug)]
    enum TestTaskStatus {
        #[display("StepOne")]
        StepOne,
        #[display("StepTwo")]
        StepTwo,
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
    #[derive(Display, Debug)]
    #[display("mock")]
    struct Mock {
        state: State,
        notify: Arc<Notify>,
        outcome: Outcome,
    }

    impl Task for Mock {
        type Status = Empty;
        type Output = ();
        type Error = std::io::Error;

        async fn run(self, _ctx: AsyncTaskContext<Self>) -> Result<(), std::io::Error> {
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
    /// token, so it is aborted immediately on cancellation.
    #[derive(Display, Debug)]
    #[display("anonymous")]
    struct Gate {
        gate: Arc<Notify>,
    }

    impl Task for Gate {
        type Status = Empty;
        type Output = ();
        type Error = Infallible;

        async fn run(self, _ctx: AsyncTaskContext<Self>) -> Result<(), Infallible> {
            self.gate.notified().await;
            Ok(())
        }
    }

    /// Immediately succeeds.
    #[derive(Display, Debug)]
    #[display("noop")]
    struct Noop;

    impl Task for Noop {
        type Status = Empty;
        type Output = ();
        type Error = Infallible;

        async fn run(self, _ctx: AsyncTaskContext<Self>) -> Result<(), Infallible> {
            Ok(())
        }
    }

    /// Runs a single child task of any type (spawned via `ctx.run`),
    /// propagating its error if it fails, then waits on `notify`
    /// before finishing.
    #[derive(Display, Debug)]
    #[display("inner")]
    struct Inner<C: Task> {
        state: State,
        notify: Arc<Notify>,
        child: C,
    }

    impl<C: Task> Task for Inner<C> {
        type Status = Empty;
        type Output = ();
        type Error = TaskError<C::Error>;

        async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<(), TaskError<C::Error>> {
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
    #[derive(Display, Debug)]
    #[display("cancellable")]
    struct Cancellable {
        state: State,
        notify: Arc<Notify>,
        wind_down: Duration,
    }

    impl Task for Cancellable {
        type Status = Empty;
        type Output = bool;
        type Error = Infallible;

        fn cancel_timeout() -> Duration {
            Duration::from_secs(30)
        }

        async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<bool, Infallible> {
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

    /// Subtask that spawns a grandchild gate (registered with the root)
    /// and waits for it.
    #[derive(Display, Debug)]
    #[display("anonymous")]
    struct Sub {
        sub_gate: Arc<Notify>,
    }

    impl Task for Sub {
        type Status = Empty;
        type Output = ();
        type Error = Infallible;

        async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<(), Infallible> {
            ctx.run(Gate {
                gate: self.sub_gate,
            })
            .await
            .unwrap();

            Ok(())
        }
    }

    /// Root task that reports `TestTaskStatus`, runs a subtask, then
    /// advances its status and waits.
    #[derive(Display, Debug)]
    #[display("test_task")]
    struct TraverseRoot {
        sub_gate: Arc<Notify>,
        parent_gate: Arc<Notify>,
    }

    impl Task for TraverseRoot {
        type Status = TestTaskStatus;
        type Output = ();
        type Error = Infallible;

        async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<(), Infallible> {
            ctx.set_status(TestTaskStatus::StepOne);

            let sub = ctx.run(Sub {
                sub_gate: self.sub_gate,
            });

            sub.await.unwrap();

            ctx.set_status(TestTaskStatus::StepTwo);

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

        let async_storage = AsyncTasksStorage::default();

        let task = async_storage.run(a);

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

        let async_storage = AsyncTasksStorage::default();

        let task_a = async_storage.run(a);
        let task_b = async_storage.run(b);

        let info = async_storage.task(task_a.id()).unwrap();

        assert_eq!(async_storage.tasks().len(), 2);

        assert!(matches!(info.state.status, TaskStatus::Started));

        let info = async_storage.task(task_b.id()).unwrap();

        assert!(matches!(info.state.status, TaskStatus::Started));

        settle().await;

        assert_eq!(*state_a.lock(), "started");
        assert_eq!(*state_b.lock(), "started");
        assert_eq!(*state_c.lock(), "initial");

        notify.notify_one();
        notify.notify_one();

        let task_c = async_storage.run(c);

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

        let async_storage = AsyncTasksStorage::default();

        let task_c = async_storage.run(c);

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

        let async_storage = AsyncTasksStorage::default();

        let task = async_storage.run(a);

        settle().await;

        assert_eq!(*state_a.lock(), "started");

        let task_id = task.id();
        async_storage.cancel_task(task_id);

        let res = task.await;
        assert!(matches!(res, Err(TaskError::Cancelled)));

        assert_eq!(*state_a.lock(), "started");

        // Cancelled tasks stay visible with a terminal status.
        let snapshot = async_storage.task(task_id).unwrap();
        assert!(matches!(snapshot.state.status, TaskStatus::Cancelled));
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

        let async_storage = AsyncTasksStorage::default();

        let task = async_storage.run(a);
        let task_id = task.id();

        settle().await;

        assert_eq!(*state.lock(), "started");

        async_storage.cancel_task(task_id);

        // The task observed the token and finished gracefully: its
        // result must be delivered, not discarded or lost to a
        // watcher panic.
        let res = task.await;
        assert!(res.unwrap());

        assert_eq!(*state.lock(), "cancelled");

        // Cancellation was requested, so the terminal status is `Cancelled`
        // even though the task wound down cleanly to `Ok(42)`.
        let snapshot = async_storage.task(task_id).unwrap();
        assert!(matches!(snapshot.state.status, TaskStatus::Cancelled));
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

        let async_storage = AsyncTasksStorage::default();

        let task = async_storage.run(a);
        let task_id = task.id();

        settle().await;
        async_storage.cancel_task(task_id);
        settle().await;

        // Cancellation requested; the task is still winding down (sleeping
        // `wind_down`) and must report a non-terminal `Cancelling` status.
        let snapshot = async_storage.task(task_id).unwrap();
        assert!(matches!(snapshot.state.status, TaskStatus::Cancelling));
        assert!(!snapshot.state.is_terminal());

        // Once it finishes winding down, the status settles to terminal.
        let res = task.await;
        assert!(res.unwrap());
        let snapshot = async_storage.task(task_id).unwrap();
        assert!(matches!(snapshot.state.status, TaskStatus::Cancelled));
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

        let async_storage = AsyncTasksStorage::default();

        let task_c = async_storage.run(c);

        settle().await;

        assert_eq!(*state_a.lock(), "started");
        assert_eq!(*state_c.lock(), "started");

        async_storage.cancel_task(task_c.id());

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

        let async_storage = AsyncTasksStorage::default();

        let task = async_storage.run(a);

        settle().await;

        assert_eq!(*state_a.lock(), "started");

        notify.notify_one();

        let task_id = task.id();
        let res = task.await;
        assert!(matches!(res, Err(TaskError::Failed(_))));

        assert_eq!(*state_a.lock(), "failed");

        let info = async_storage.task(task_id).unwrap();
        assert!(matches!(info.state.status, TaskStatus::Error(_)));
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

        let async_storage = AsyncTasksStorage::default();

        let task_c = async_storage.run(c);

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

        let async_storage = AsyncTasksStorage::default();

        let task = async_storage.run(a);

        settle().await;

        assert_eq!(*state_a.lock(), "started");

        notify.notify_one();

        let task_id = task.id();
        let res = task.await;
        assert!(matches!(res, Err(TaskError::Panicked(_))));

        assert_eq!(*state_a.lock(), "started");

        let info = async_storage.task(task_id).unwrap();
        assert!(matches!(info.state.status, TaskStatus::Panic(_)));
    }

    #[test]
    async fn test_traverse_statuses() {
        let sub_gate = Arc::new(Notify::new());
        let parent_gate = Arc::new(Notify::new());

        let async_storage = AsyncTasksStorage::default();

        let task = async_storage.run(TraverseRoot {
            sub_gate: sub_gate.clone(),
            parent_gate: parent_gate.clone(),
        });

        let id = task.id();

        settle().await;

        // Top-level listing: one named task with a live status.
        let tasks = async_storage.tasks();
        assert_eq!(tasks.len(), 1);

        let snapshot = &tasks[0];
        assert_eq!(snapshot.id, id);
        assert_eq!(snapshot.state.name, "test_task");
        assert!(
            snapshot.state.status == TaskStatus::Running
                && snapshot.state.inner_status.as_deref() == Some("StepOne")
        );

        // Both the subtask and its grandchild appear as direct
        // children of the root, flat.
        assert_eq!(snapshot.subtasks.len(), 2);
        for sub in &snapshot.subtasks {
            assert_eq!(sub.state.name, "anonymous");
            assert!(matches!(sub.state.status, TaskStatus::Started));
            assert!(sub.subtasks.is_empty());
        }

        sub_gate.notify_one();
        settle().await;

        // Subtask finished, parent moved to the next phase.
        let snapshot = async_storage.task(id).unwrap();
        assert!(
            snapshot.state.status == TaskStatus::Running
                && snapshot.state.inner_status.as_deref() == Some("StepTwo")
        );
        for sub in &snapshot.subtasks {
            assert!(matches!(sub.state.status, TaskStatus::Finished));
        }

        parent_gate.notify_one();

        task.await.unwrap();

        // Terminal status stays observable after completion, and the last
        // inner progress is preserved across the terminal transition.
        let snapshot = async_storage.task(id).unwrap();
        assert!(matches!(snapshot.state.status, TaskStatus::Finished));
        assert_eq!(snapshot.state.inner_status.as_deref(), Some("StepTwo"));
    }

    #[test]
    async fn test_prune_expired_tasks_and_subtasks() {
        let notify_a = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let a = mock_successful!(state_a, notify_a);

        let sub_gate = Arc::new(Notify::new());
        let parent_gate = Arc::new(Notify::new());

        // Zero retention: terminal tasks are pruned on next access.
        let async_storage = AsyncTasksStorage::new(Duration::ZERO);

        let task_a = async_storage.run(a);
        let id_a = task_a.id();

        let root = async_storage.run(TraverseRoot {
            sub_gate: sub_gate.clone(),
            parent_gate: parent_gate.clone(),
        });
        let root_id = root.id();

        settle().await;

        // Both roots running; the root has its two descendants registered.
        assert_eq!(async_storage.tasks().len(), 2);
        assert_eq!(async_storage.task(root_id).unwrap().subtasks.len(), 2);

        // Finish the top-level task: it expired and is pruned on next
        // access, while the still-running root survives.
        notify_a.notify_one();
        task_a.await.unwrap();

        let tasks = async_storage.tasks();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].id, root_id);
        assert!(async_storage.task(id_a).is_none());

        // Let the subtasks finish; the root parks on `parent_gate`, still running.
        sub_gate.notify_one();
        settle().await;

        // The finished subtasks are pruned while the still-running root survives.
        let snapshot = async_storage.task(root_id).unwrap();
        assert!(matches!(snapshot.state.status, TaskStatus::Running));
        assert!(snapshot.subtasks.is_empty());

        // The root finishes and expires too: the registry empties.
        parent_gate.notify_one();
        root.await.unwrap();

        assert!(async_storage.tasks().is_empty());
    }

    #[test(start_paused = true)]
    async fn test_cancel_timeout_override() {
        let notify = Arc::new(Notify::new());

        let async_storage = AsyncTasksStorage::default();

        let task = async_storage.run(Cancellable {
            state: Arc::new(Mutex::new("initial")),
            notify: notify.clone(),
            wind_down: Duration::from_secs(60),
        });

        settle().await;

        let started = tokio::time::Instant::now();

        async_storage.cancel_task(task.id());

        let res = task.await;
        assert!(matches!(res, Err(TaskError::Cancelled)));

        // The paused clock advances exactly by the overridden
        // grace period, not the default 5s.
        assert_eq!(started.elapsed(), Duration::from_secs(30));
    }

    #[test(start_paused = true)]
    async fn test_non_cooperative_cancel_aborts_immediately() {
        let notify = Arc::new(Notify::new());

        let async_storage = AsyncTasksStorage::default();

        // `Gate` never takes the cancellation token: no grace period.
        let task = async_storage.run(Gate {
            gate: notify.clone(),
        });

        settle().await;

        let started = tokio::time::Instant::now();

        async_storage.cancel_task(task.id());

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

        let async_storage = AsyncTasksStorage::default();
        let task = async_storage.run(a);
        let id = task.id();

        notify.notify_one();
        task.await.unwrap();

        // The task is terminal (finished) but still retained for reporting:
        // cancelling it now reports not-found, so STOP_TASK won't claim it
        // stopped a completed task (nor emit a bogus cleanup warning).
        assert!(async_storage.task(id).is_some());
        assert!(async_storage.cancel_task(id).is_none());

        // An unknown id is not-found too.
        assert!(
            async_storage
                .cancel_task(AsyncTaskId::from(99_u64))
                .is_none()
        );
    }
}
