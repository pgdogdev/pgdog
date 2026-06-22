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
use tokio::select;
use tokio::sync::oneshot::{self, Receiver};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

#[derive(Copy, Clone, Debug, Display, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct AsyncTaskId(u64);

impl From<u64> for AsyncTaskId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<AsyncTaskId> for u64 {
    fn from(id: AsyncTaskId) -> Self {
        id.0
    }
}

/// Status type for tasks that report no intermediate progress.
///
/// [`Infallible`](std::convert::Infallible) is uninhabited, so a task
/// with this status type can never call
/// [`set_status`](AsyncTaskContext::set_status).
pub type Empty = std::convert::Infallible;

/// A composable background task: a value carrying its own arguments,
/// driven to completion by [`run`](Task::run). Launch it top-level with
/// [`AsyncTasksStorage::run`], or nested under a running task with
/// [`AsyncTaskContext::run`].
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

    fn run(
        self,
        ctx: AsyncTaskContext<Self>,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send + 'static;
}

#[derive(Display, Debug, Clone)]
pub enum TaskStatus<S = Empty> {
    Started,
    Pending(S),
    Finished,
    Cancelled,
    Error(String),
    Panic(String),
}

/// Type-erased snapshot of a task's current state,
/// readable through the registry without knowing `T`.
#[derive(Debug, Clone)]
pub struct TaskState {
    pub name: String,
    pub status: TaskStatus<String>,
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
    #[display("task was cancelled")]
    Cancelled,
    #[display("task panicked: {_0}")]
    Panicked(#[error(ignore)] String),
    /// The task's result was never delivered: the watcher
    /// died without reporting (e.g. runtime shutdown).
    #[display("task result was never delivered")]
    Abandoned,
}

impl<S> TaskStatus<S> {
    /// Terminal states are write-once; late writers
    /// (e.g. ctx clones outliving the task) are ignored.
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Finished | Self::Cancelled | Self::Error(_) | Self::Panic(_)
        )
    }

    /// Snapshot for the registry: keep the variant, render `T`.
    fn stringify(&self) -> TaskStatus<String>
    where
        S: Display,
    {
        match self {
            Self::Started => TaskStatus::Started,
            Self::Pending(status) => TaskStatus::Pending(status.to_string()),
            Self::Finished => TaskStatus::Finished,
            Self::Cancelled => TaskStatus::Cancelled,
            Self::Error(err) => TaskStatus::Error(err.clone()),
            Self::Panic(msg) => TaskStatus::Panic(msg.clone()),
        }
    }
}

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

struct AsyncTaskState<S = Empty> {
    updated_at: SystemTime,
    status: TaskStatus<S>,
}

impl<S> AsyncTaskState<S> {
    fn new() -> Self {
        Self {
            updated_at: SystemTime::now(),
            status: TaskStatus::Started,
        }
    }
}

struct AsyncTask<T: Task> {
    started_at: SystemTime,
    /// Id of the root task this task belongs to — its own id when it is a
    /// root, inherited from the parent otherwise. Stable for the task's
    /// lifetime and used to address its cutover signal.
    root_id: AsyncTaskId,
    name: String,
    cancellation_token: CancellationToken,
    /// Set once the task asks for its cancellation token: only
    /// then can it react to cancellation, so only then is the
    /// cooperative-shutdown grace period worth waiting out.
    cooperative: AtomicBool,
    state: Arc<RwLock<AsyncTaskState<T::Status>>>,
    subtasks: Arc<TasksMap>,
}

trait TaskMapEntry: Send + Sync + 'static {
    fn cancel(&self);
    fn state(&self) -> TaskState;
    fn subtasks(&self) -> &TasksMap;
    /// Cheap expiry check for pruning: terminal and older than `ttl`,
    /// without building a full [`TaskState`].
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
            status: state.status.stringify(),
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
pub struct AsyncTaskWaiter<R, E> {
    id: AsyncTaskId,
    waiter: Receiver<Result<R, TaskError<E>>>,
}

impl<R, E> AsyncTaskWaiter<R, E> {
    pub fn id(&self) -> AsyncTaskId {
        self.id
    }
}

impl<R, E> Debug for AsyncTaskWaiter<R, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncTaskWaiter")
            .field("id", &self.id)
            .finish_non_exhaustive()
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

pub struct AsyncTasksStorage {
    tasks: Arc<TasksMap>,
    retention: Duration,
}

impl Default for AsyncTasksStorage {
    fn default() -> Self {
        Self::new(TASK_RETENTION)
    }
}

fn run_task<T: Task>(
    parent_token: Option<&CancellationToken>,
    register_into: &TasksMap,
    subtasks: Arc<TasksMap>,
    root: Option<AsyncTaskId>,
    task: T,
) -> AsyncTaskWaiter<T::Output, T::Error> {
    // Allocate the id up front so a root task can record its own id as its
    // root id; descendants inherit the root's id from their parent.
    let id = register_into.next_id();
    let root_id = root.unwrap_or(id);

    let state = Arc::new(RwLock::new(AsyncTaskState::new()));

    let entry = AsyncTask {
        started_at: SystemTime::now(),
        name: task.to_string(),
        root_id,
        cancellation_token: match parent_token {
            Some(token) => token.child_token(),
            None => CancellationToken::new(),
        },
        cooperative: AtomicBool::new(false),
        // Descendants share the root task's registry: every descendant
        // registers as a direct child of the root.
        subtasks,
        state: state.clone(),
    };

    let entry = Arc::new(entry);
    // Make sure we insert task to map before it's actually started.
    register_into.insert(id, entry.clone());

    let ctx = AsyncTaskContext {
        task: entry.clone(),
    };

    let mut handle = tokio::spawn(task.run(ctx.clone()));
    let (sender, receiver) = oneshot::channel();

    let cancellation_token = entry.cancellation_token.clone();

    tokio::spawn(async move {
        let res = select! {
            _ = cancellation_token.cancelled() => {
                if ctx.task.cooperative.load(Ordering::Relaxed) {
                    match timeout(T::cancel_timeout(), &mut handle).await {
                        Ok(res) => res,
                        Err(_) => {
                            handle.abort();
                            handle.await
                        }
                    }
                } else {
                    // The task never took its cancellation token, so it
                    // cannot react to it: abort right away instead of
                    // letting it run on through the grace period.
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
                ctx.set_inner_status(TaskStatus::Finished);
                let _ = sender.send(Ok(res));
            }
            Ok(Err(err)) => {
                ctx.set_inner_status(TaskStatus::Error(err.to_string()));
                let _ = sender.send(Err(TaskError::Failed(err)));
            }
            Err(err) if err.is_cancelled() => {
                ctx.set_inner_status(TaskStatus::Cancelled);
                let _ = sender.send(Err(TaskError::Cancelled));
            }
            Err(err) => {
                let panic = err.to_string();
                ctx.set_inner_status(TaskStatus::Panic(panic.clone()));
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
    fn set_inner_status(&self, status: TaskStatus<T::Status>) {
        let mut state = self.task.state.write();
        if state.status.is_terminal() {
            return;
        }
        state.status = status;
        state.updated_at = SystemTime::now();
    }

    pub fn set_status(&self, status: T::Status) {
        self.set_inner_status(TaskStatus::Pending(status));
    }

    /// Hand out this task's cancellation token. Taking the token
    /// opts the task into cooperative shutdown: on `cancel_task`
    /// it gets [`Task::cancel_timeout`] to wind down
    /// before being aborted. Tasks that never take it are aborted
    /// immediately. Take it early.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.task.cooperative.store(true, Ordering::Relaxed);

        self.task.cancellation_token.clone()
    }

    pub fn run<T1: Task>(&self, task: T1) -> AsyncTaskWaiter<T1::Output, T1::Error> {
        run_task(
            Some(&self.task.cancellation_token),
            &self.task.subtasks,
            self.task.subtasks.clone(),
            Some(self.task.root_id),
            task,
        )
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

    pub fn run<T: Task>(&self, task: T) -> AsyncTaskWaiter<T::Output, T::Error> {
        self.prune();

        run_task(None, &self.tasks, Arc::new(TasksMap::default()), None, task)
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
    use std::sync::Arc;
    use tokio::sync::Notify;
    use tokio::task::yield_now;
    use tokio::time::sleep;
    use tokio::{join, test};

    type State = Arc<Mutex<&'static str>>;

    #[derive(Display, Debug)]
    enum TestTaskStatus {
        #[display("StepOne")]
        StepOne,
        #[display("StepTwo")]
        StepTwo,
    }

    /// Sets "started", waits on `notify`, then "finished" and succeeds.
    #[derive(Display, Debug)]
    #[display("mock")]
    struct MockSuccessful {
        state: State,
        notify: Arc<Notify>,
    }

    impl Task for MockSuccessful {
        type Status = Empty;
        type Output = ();
        type Error = Infallible;

        async fn run(self, _ctx: AsyncTaskContext<Self>) -> Result<(), Infallible> {
            *self.state.lock() = "started";
            self.notify.notified().await;
            *self.state.lock() = "finished";
            Ok(())
        }
    }

    /// Sets "started", waits on `notify`, then "failed" and errors.
    #[derive(Display, Debug)]
    #[display("mock")]
    struct MockFailing {
        state: State,
        notify: Arc<Notify>,
    }

    impl Task for MockFailing {
        type Status = Empty;
        type Output = ();
        type Error = std::io::Error;

        async fn run(self, _ctx: AsyncTaskContext<Self>) -> Result<(), std::io::Error> {
            *self.state.lock() = "started";
            self.notify.notified().await;
            *self.state.lock() = "failed";
            Err(std::io::Error::other("mock task failure"))
        }
    }

    macro_rules! mock_successful {
        ($state:ident, $notify:ident) => {
            MockSuccessful {
                state: $state.clone(),
                notify: $notify.clone(),
            }
        };
    }

    macro_rules! mock_failing {
        ($state:ident, $notify:ident) => {
            MockFailing {
                state: $state.clone(),
                notify: $notify.clone(),
            }
        };
    }

    /// Waits on its gate, then succeeds. Never takes its cancellation
    /// token, so it is aborted (not wound down) on cancellation.
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

    /// Runs two children concurrently and joins them, then finishes.
    #[derive(Display, Debug)]
    #[display("inner")]
    struct InnerJoin {
        state: State,
        a: MockSuccessful,
        b: MockSuccessful,
    }

    impl Task for InnerJoin {
        type Status = Empty;
        type Output = ();
        type Error = Infallible;

        async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<(), Infallible> {
            *self.state.lock() = "started";

            let (a, b) = join!(ctx.run(self.a), ctx.run(self.b));
            a.unwrap();
            b.unwrap();

            *self.state.lock() = "finished";

            Ok(())
        }
    }

    /// Runs two children concurrently, then waits on `notify`.
    #[derive(Display, Debug)]
    #[display("inner")]
    struct InnerCancel {
        state: State,
        notify: Arc<Notify>,
        a: MockSuccessful,
        b: MockSuccessful,
    }

    impl Task for InnerCancel {
        type Status = Empty;
        type Output = ();
        type Error = Infallible;

        async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<(), Infallible> {
            *self.state.lock() = "started";

            let _ = join!(ctx.run(self.a), ctx.run(self.b));

            self.notify.notified().await;

            *self.state.lock() = "finished";

            Ok(())
        }
    }

    /// Runs a failing child and asserts it failed.
    #[derive(Display, Debug)]
    #[display("inner")]
    struct InnerError {
        state: State,
        a: MockFailing,
    }

    impl Task for InnerError {
        type Status = Empty;
        type Output = ();
        type Error = Infallible;

        async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<(), Infallible> {
            *self.state.lock() = "started";

            let res = ctx.run(self.a).await;
            assert!(matches!(res, Err(TaskError::Failed(_))));

            *self.state.lock() = "inner_failed";

            Ok(())
        }
    }

    /// Takes its cancellation token and winds down gracefully within
    /// the grace window, then succeeds with a value.
    #[derive(Display, Debug)]
    #[display("graceful")]
    struct Graceful {
        state: State,
    }

    impl Task for Graceful {
        type Status = Empty;
        type Output = i32;
        type Error = Infallible;

        async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<i32, Infallible> {
            *self.state.lock() = "started";

            ctx.cancellation_token().cancelled().await;

            // Cooperative wind-down, well within the 5s grace window.
            sleep(Duration::from_secs(1)).await;

            *self.state.lock() = "graceful";

            Ok(42)
        }
    }

    /// Panics after being notified, if still in the "started" state.
    #[derive(Display, Debug)]
    #[display("panicker")]
    struct Panicker {
        state: State,
        notify: Arc<Notify>,
    }

    impl Task for Panicker {
        type Status = Empty;
        type Output = ();
        type Error = Infallible;

        async fn run(self, _ctx: AsyncTaskContext<Self>) -> Result<(), Infallible> {
            *self.state.lock() = "started";

            self.notify.notified().await;

            if *self.state.lock() == "started" {
                panic!("panicking task");
            }

            Ok(())
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

    /// Takes its cancellation token (opting into the grace period) but
    /// ignores it, with an overridden 30s grace period.
    #[derive(Display, Debug)]
    #[display("slow_exit")]
    struct SlowExit {
        notify: Arc<Notify>,
    }

    impl Task for SlowExit {
        type Status = Empty;
        type Output = ();
        type Error = Infallible;

        fn cancel_timeout() -> Duration {
            Duration::from_secs(30)
        }

        async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<(), Infallible> {
            let _token = ctx.cancellation_token();
            self.notify.notified().await;
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
        let notify = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let state_b = Arc::new(Mutex::new("initial"));
        let state_c = Arc::new(Mutex::new("initial"));
        let c = InnerJoin {
            state: state_c.clone(),
            a: mock_successful!(state_a, notify),
            b: mock_successful!(state_b, notify),
        };

        let async_storage = AsyncTasksStorage::default();

        let task_c = async_storage.run(c);

        settle().await;

        assert_eq!(*state_a.lock(), "started");
        assert_eq!(*state_b.lock(), "started");
        assert_eq!(*state_c.lock(), "started");

        notify.notify_waiters();

        task_c.await.unwrap();

        assert_eq!(*state_a.lock(), "finished");
        assert_eq!(*state_b.lock(), "finished");
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
        let state = Arc::new(Mutex::new("initial"));
        let a = Graceful {
            state: state.clone(),
        };

        let async_storage = AsyncTasksStorage::default();

        let task = async_storage.run(a);

        settle().await;

        assert_eq!(*state.lock(), "started");

        async_storage.cancel_task(task.id());

        // The task observed the token and finished gracefully: its
        // result must be delivered, not discarded or lost to a
        // watcher panic.
        let res = task.await;
        assert_eq!(res.unwrap(), 42);

        assert_eq!(*state.lock(), "graceful");
    }

    #[test(start_paused = true)]
    async fn test_inner_cancel() {
        let notify = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let state_b = Arc::new(Mutex::new("initial"));
        let state_c = Arc::new(Mutex::new("initial"));
        let c = InnerCancel {
            state: state_c.clone(),
            notify: notify.clone(),
            a: mock_successful!(state_a, notify),
            b: mock_successful!(state_b, notify),
        };

        let async_storage = AsyncTasksStorage::default();

        let task_c = async_storage.run(c);

        settle().await;

        assert_eq!(*state_a.lock(), "started");
        assert_eq!(*state_b.lock(), "started");
        assert_eq!(*state_c.lock(), "started");

        async_storage.cancel_task(task_c.id());

        let res = task_c.await;
        assert!(matches!(res, Err(TaskError::Cancelled)));

        assert_eq!(*state_a.lock(), "started");
        assert_eq!(*state_b.lock(), "started");
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
        let c = InnerError {
            state: state_c.clone(),
            a: mock_failing!(state_a, notify),
        };

        let async_storage = AsyncTasksStorage::default();

        let task_c = async_storage.run(c);

        settle().await;

        assert_eq!(*state_a.lock(), "started");
        assert_eq!(*state_c.lock(), "started");

        notify.notify_one();

        task_c.await.unwrap();

        assert_eq!(*state_a.lock(), "failed");
        assert_eq!(*state_c.lock(), "inner_failed");
    }

    #[test]
    async fn test_panic() {
        let notify = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let a = Panicker {
            state: state_a.clone(),
            notify: notify.clone(),
        };

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
            matches!(&snapshot.state.status, TaskStatus::Pending(s) if s.as_str() == "StepOne")
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
            matches!(&snapshot.state.status, TaskStatus::Pending(s) if s.as_str() == "StepTwo")
        );
        for sub in &snapshot.subtasks {
            assert!(matches!(sub.state.status, TaskStatus::Finished));
        }

        parent_gate.notify_one();

        task.await.unwrap();

        // Terminal status stays observable after completion.
        let snapshot = async_storage.task(id).unwrap();
        assert!(matches!(snapshot.state.status, TaskStatus::Finished));
    }

    #[test]
    async fn test_prune_expired_tasks() {
        let notify_a = Arc::new(Notify::new());
        let notify_b = Arc::new(Notify::new());
        let state_a = Arc::new(Mutex::new("initial"));
        let state_b = Arc::new(Mutex::new("initial"));
        let a = mock_successful!(state_a, notify_a);
        let b = mock_successful!(state_b, notify_b);

        // Zero retention: terminal tasks are pruned on next access.
        let async_storage = AsyncTasksStorage::new(Duration::ZERO);

        let task_a = async_storage.run(a);
        let task_b = async_storage.run(b);
        let id_a = task_a.id();
        let id_b = task_b.id();

        settle().await;

        // Both running: nothing to prune.
        assert_eq!(async_storage.tasks().len(), 2);

        notify_a.notify_one();
        task_a.await.unwrap();

        // The finished task expired; the running one survives.
        let tasks = async_storage.tasks();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].id, id_b);
        assert!(async_storage.task(id_a).is_none());

        notify_b.notify_one();
        task_b.await.unwrap();

        assert!(async_storage.tasks().is_empty());
    }

    #[test(start_paused = true)]
    async fn test_cancel_timeout_override() {
        let notify = Arc::new(Notify::new());

        let async_storage = AsyncTasksStorage::default();

        let task = async_storage.run(SlowExit {
            notify: notify.clone(),
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
        let waiter = crate::api::storage().run(Noop);
        let id = waiter.id();
        waiter.await.unwrap();
        assert!(crate::api::storage().task(id).is_some());
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

    #[test]
    async fn test_prune_expired_subtasks_under_running_root() {
        let sub_gate = Arc::new(Notify::new());
        let parent_gate = Arc::new(Notify::new());

        // Zero retention: terminal tasks are pruned on next access.
        let async_storage = AsyncTasksStorage::new(Duration::ZERO);

        let task = async_storage.run(TraverseRoot {
            sub_gate: sub_gate.clone(),
            parent_gate: parent_gate.clone(),
        });
        let root_id = task.id();

        settle().await;

        // Root running with its two descendants registered.
        assert_eq!(async_storage.task(root_id).unwrap().subtasks.len(), 2);

        // Let the subtasks finish; the root parks on `parent_gate`, still running.
        sub_gate.notify_one();
        settle().await;

        // The finished subtasks are pruned while the still-running root survives.
        let snapshot = async_storage.task(root_id).unwrap();
        assert!(matches!(snapshot.state.status, TaskStatus::Pending(_)));
        assert!(snapshot.subtasks.is_empty());

        parent_gate.notify_one();
        task.await.unwrap();
    }
}
