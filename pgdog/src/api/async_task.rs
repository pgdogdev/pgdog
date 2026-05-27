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
pub trait TaskInfoStatus: Display + Debug + Send + Sync + 'static {
    fn task_name() -> &'static str;

    /// Grace period for cooperative shutdown after cancellation;
    /// once it expires, the task is force-aborted.
    fn cancel_timeout() -> Duration {
        Duration::from_secs(5)
    }
}

#[derive(Display, Debug)]
pub struct AnonymousTask;

impl TaskInfoStatus for AnonymousTask {
    fn task_name() -> &'static str {
        "anonymous"
    }
}

#[derive(Display, Debug, Clone)]
pub enum TaskStatus<T> {
    Started,
    Pending(T),
    Finished,
    Cancelled,
    Error(String),
    Panic(String),
}

/// Type-erased snapshot of a task's current state,
/// readable through the registry without knowing `T`.
#[derive(Debug, Clone)]
pub struct TaskState {
    pub name: &'static str,
    pub status: TaskStatus<String>,
    pub started_at: SystemTime,
    pub updated_at: SystemTime,
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

impl<T> TaskStatus<T> {
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
        T: Display,
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

impl TaskState {
    /// Reached a terminal state more than `ttl` ago?
    fn expired(&self, now: SystemTime, ttl: Duration) -> bool {
        self.status.is_terminal()
            && now
                .duration_since(self.updated_at)
                .is_ok_and(|age| age >= ttl)
    }
}

type SharedStatus<T> = Arc<RwLock<TaskStatus<T>>>;

#[derive(Default)]
struct TasksMap {
    map: DashMap<AsyncTaskId, Arc<dyn TaskMapEntry>>,
    counter: AtomicU64,
}

impl TasksMap {
    fn insert_next(&self, value: Arc<dyn TaskMapEntry>) -> AsyncTaskId {
        let id = AsyncTaskId(self.counter.fetch_add(1, Ordering::Relaxed));

        self.map.insert(id, value);

        id
    }
}

struct AsyncTaskState<T> {
    updated_at: SystemTime,
    status: TaskStatus<T>,
}

impl<T> AsyncTaskState<T> {
    fn new() -> Self {
        Self {
            updated_at: SystemTime::now(),
            status: TaskStatus::Started,
        }
    }
}

struct AsyncTask<T> {
    started_at: SystemTime,
    cancellation_token: CancellationToken,
    /// Set once the task asks for its cancellation token: only
    /// then can it react to cancellation, so only then is the
    /// cooperative-shutdown grace period worth waiting out.
    cooperative: AtomicBool,
    state: Arc<RwLock<AsyncTaskState<T>>>,
    subtasks: Arc<TasksMap>,
}

trait TaskMapEntry: Send + Sync + 'static {
    fn cancel(&self);
    fn state(&self) -> TaskState;
    fn subtasks(&self) -> &TasksMap;
}

impl<T: TaskInfoStatus> TaskMapEntry for AsyncTask<T> {
    fn cancel(&self) {
        self.cancellation_token.cancel();
    }

    fn state(&self) -> TaskState {
        let state = self.state.read();

        TaskState {
            name: T::task_name(),
            status: state.status.stringify(),
            started_at: self.started_at,
            updated_at: state.updated_at,
        }
    }

    fn subtasks(&self) -> &TasksMap {
        &self.subtasks
    }
}

pub struct AsyncTaskContext<T = AnonymousTask> {
    task: Arc<AsyncTask<T>>,
}

impl<T> Clone for AsyncTaskContext<T> {
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

fn run_task<T, T1, F, R, E>(
    current_task: Option<&Arc<AsyncTask<T>>>,
    tasks: &TasksMap,
    execute: impl FnOnce(AsyncTaskContext<T1>) -> F,
) -> AsyncTaskWaiter<R, E>
where
    T: TaskInfoStatus,
    T1: TaskInfoStatus,
    F: Future<Output = Result<R, E>> + Send + 'static,
    R: Send + 'static,
    E: std::error::Error + Send + 'static,
{
    let state = Arc::new(RwLock::new(AsyncTaskState::new()));

    let task = AsyncTask {
        started_at: SystemTime::now(),
        cancellation_token: match current_task {
            Some(current_task) => current_task.cancellation_token.child_token(),
            None => CancellationToken::new(),
        },
        cooperative: AtomicBool::new(false),
        // Subtasks share the root task's registry: every descendant
        // registers as a direct child of the root.
        subtasks: match current_task {
            Some(current_task) => current_task.subtasks.clone(),
            None => Arc::new(TasksMap::default()),
        },
        state: state.clone(),
    };

    let task = Arc::new(task);
    // Make sure we insert task to map before it's actually started
    let id = tasks.insert_next(task.clone());

    let ctx = AsyncTaskContext { task: task.clone() };

    let mut handle = tokio::spawn(execute(ctx.clone()));
    let (sender, receiver) = oneshot::channel();

    let cancellation_token = task.cancellation_token.clone();

    tokio::spawn(async move {
        let res = select! {
            _ = cancellation_token.cancelled() => {
                if ctx.task.cooperative.load(Ordering::Relaxed) {
                    match timeout(T1::cancel_timeout(), &mut handle).await {
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

impl<T: TaskInfoStatus> AsyncTaskContext<T> {
    fn set_inner_status(&self, status: TaskStatus<T>) {
        let mut state = self.task.state.write();
        if state.status.is_terminal() {
            return;
        }
        state.status = status;
        state.updated_at = SystemTime::now();
    }

    pub fn set_status(&self, status: T) {
        self.set_inner_status(TaskStatus::Pending(status));
    }

    /// Hand out this task's cancellation token. Taking the token
    /// opts the task into cooperative shutdown: on `cancel_task`
    /// it gets [`TaskInfoStatus::cancel_timeout`] to wind down
    /// before being aborted. Tasks that never take it are aborted
    /// immediately. Take it early.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.task.cooperative.store(true, Ordering::Relaxed);

        self.task.cancellation_token.clone()
    }

    pub fn run<T1, F, R, E>(
        &self,
        execute: impl FnOnce(AsyncTaskContext<T1>) -> F,
    ) -> AsyncTaskWaiter<R, E>
    where
        T1: TaskInfoStatus,
        F: Future<Output = Result<R, E>> + Send + 'static,
        R: Send + 'static,
        E: std::error::Error + Send + 'static,
    {
        run_task(Some(&self.task), &self.task.subtasks, execute)
    }
}

impl AsyncTasksStorage {
    pub fn new(retention: Duration) -> Self {
        Self {
            tasks: Arc::default(),
            retention,
        }
    }

    pub fn run<T, F, R, E>(
        &self,
        execute: impl FnOnce(AsyncTaskContext<T>) -> F,
    ) -> AsyncTaskWaiter<R, E>
    where
        T: TaskInfoStatus,
        F: Future<Output = Result<R, E>> + Send + 'static,
        R: Send + 'static,
        E: std::error::Error + Send + 'static,
    {
        self.prune();

        run_task(Option::<&Arc<AsyncTask<T>>>::None, &self.tasks, execute)
    }

    /// Request cancellation of a task. The task winds down
    /// cooperatively (or is aborted after the grace period) and
    /// stays in the registry with a terminal status until pruned.
    /// Returns the state the task was in when cancellation was
    /// requested, or `None` for an unknown id.
    pub fn cancel_task(&self, id: AsyncTaskId) -> Option<TaskState> {
        let entry = self.tasks.map.get(&id)?;

        entry.cancel();

        Some(entry.state())
    }

    /// Drop every task that reached a terminal state more than
    /// `retention` ago; running tasks are never dropped.
    fn prune(&self) {
        let now = SystemTime::now();

        self.tasks.map.retain(|_, entry| {
            entry
                .subtasks()
                .map
                .retain(|_, sub| !sub.state().expired(now, self.retention));

            !entry.state().expired(now, self.retention)
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

    #[derive(Display, Debug)]
    enum TestTask {
        StepOne,
        StepTwo,
    }

    impl TaskInfoStatus for TestTask {
        fn task_name() -> &'static str {
            "test_task"
        }
    }

    macro_rules! mock_successful {
        ($state_id:ident, $notify:ident) => {{
            let state = $state_id.clone();
            let notify = $notify.clone();

            async move |_ctx: AsyncTaskContext| {
                *state.lock() = "started";

                notify.notified().await;

                *state.lock() = "finished";

                Ok::<_, Infallible>(())
            }
        }};
    }

    macro_rules! mock_failing {
        ($state_id:ident, $notify:ident) => {{
            let state = $state_id.clone();
            let notify = $notify.clone();

            async move |_ctx: AsyncTaskContext| {
                *state.lock() = "started";

                notify.notified().await;

                *state.lock() = "failed";

                Err::<(), _>(std::io::Error::other("mock task failure"))
            }
        }};
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
        let a = mock_successful!(state_a, notify);
        let b = mock_successful!(state_b, notify);
        let c = {
            let state = state_c.clone();

            async move |ctx: AsyncTaskContext| {
                *state.lock() = "started";

                let (a, b) = join!(ctx.run(a), ctx.run(b));
                a.unwrap();
                b.unwrap();

                *state.lock() = "finished";

                Ok::<_, Infallible>(())
            }
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
        let a = {
            let state = state.clone();

            async move |ctx: AsyncTaskContext| {
                *state.lock() = "started";

                ctx.cancellation_token().cancelled().await;

                // Cooperative wind-down, well within the 5s grace window.
                sleep(Duration::from_secs(1)).await;

                *state.lock() = "graceful";

                Ok::<_, Infallible>(42)
            }
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
        let a = mock_successful!(state_a, notify);
        let b = mock_successful!(state_b, notify);
        let c = {
            let state = state_c.clone();
            let notify = notify.clone();

            async move |ctx: AsyncTaskContext| {
                *state.lock() = "started";

                let _ = join!(ctx.run(a), ctx.run(b));

                notify.notified().await;

                *state.lock() = "finished";

                Ok::<_, Infallible>(())
            }
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
        let a = mock_failing!(state_a, notify);
        let c = {
            let state = state_c.clone();

            async move |ctx: AsyncTaskContext| {
                *state.lock() = "started";

                let res = ctx.run(a).await;
                assert!(matches!(res, Err(TaskError::Failed(_))));

                *state.lock() = "inner_failed";

                Ok::<_, Infallible>(())
            }
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
        let a = {
            let state = state_a.clone();
            let notify = notify.clone();

            async move |_ctx: AsyncTaskContext| {
                *state.lock() = "started";

                notify.notified().await;

                if *state.lock() == "started" {
                    panic!("panicking task");
                }

                Ok::<_, Infallible>(())
            }
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

        let task = async_storage.run({
            let sub_gate = sub_gate.clone();
            let parent_gate = parent_gate.clone();

            async move |ctx: AsyncTaskContext<TestTask>| {
                ctx.set_status(TestTask::StepOne);

                let sub = ctx.run({
                    let sub_gate = sub_gate.clone();

                    async move |ctx: AsyncTaskContext| {
                        // Grandchild: registers with the root task.
                        ctx.run(async move |_ctx: AsyncTaskContext| {
                            sub_gate.notified().await;
                            Ok::<_, Infallible>(())
                        })
                        .await
                        .unwrap();

                        Ok::<_, Infallible>(())
                    }
                });

                sub.await.unwrap();

                ctx.set_status(TestTask::StepTwo);

                parent_gate.notified().await;

                Ok::<_, Infallible>(())
            }
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
        #[derive(Display, Debug)]
        struct SlowExitTask;

        impl TaskInfoStatus for SlowExitTask {
            fn task_name() -> &'static str {
                "slow_exit"
            }

            fn cancel_timeout() -> Duration {
                Duration::from_secs(30)
            }
        }

        let notify = Arc::new(Notify::new());

        let async_storage = AsyncTasksStorage::default();

        let task = async_storage.run({
            let notify = notify.clone();

            // Takes its cancellation token (opting into the grace
            // period) but ignores it: only the forced abort after
            // the grace period can stop it.
            async move |ctx: AsyncTaskContext<SlowExitTask>| {
                let _token = ctx.cancellation_token();
                notify.notified().await;
                Ok::<_, Infallible>(())
            }
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

        let task = async_storage.run({
            let notify = notify.clone();

            // Never takes the cancellation token: no grace period.
            async move |_ctx: AsyncTaskContext| {
                notify.notified().await;
                Ok::<_, Infallible>(())
            }
        });

        settle().await;

        let started = tokio::time::Instant::now();

        async_storage.cancel_task(task.id());

        let res = task.await;
        assert!(matches!(res, Err(TaskError::Cancelled)));

        // Aborted right away: the paused clock did not advance.
        assert_eq!(started.elapsed(), Duration::ZERO);
    }
}
