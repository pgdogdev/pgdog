use std::{
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::SystemTime,
};

use crate::backend::replication::orchestrator::ReplicationWaiter;

use super::Error;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use tokio::{
    select, spawn,
    sync::{oneshot, Notify},
    task::JoinHandle,
};
use tracing::error;

static TASKS: Lazy<AsyncTasks> = Lazy::new(AsyncTasks::default);

pub struct Task;

impl Task {
    pub(crate) fn register(task: TaskType) -> u64 {
        AsyncTasks::insert(task)
    }
}

pub enum TaskType {
    SchemaSync(JoinHandle<Result<(), Error>>),
    CopyData(JoinHandle<Result<(), Error>>),
    Replication(Box<ReplicationWaiter>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    SchemaSync,
    CopyData,
    Replication,
}

impl fmt::Display for TaskKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskKind::SchemaSync => write!(f, "schema_sync"),
            TaskKind::CopyData => write!(f, "copy_data"),
            TaskKind::Replication => write!(f, "replication"),
        }
    }
}

pub struct TaskInfo {
    #[allow(dead_code)]
    abort_tx: oneshot::Sender<()>,
    cutover: Arc<Notify>,
    pub task_kind: TaskKind,
    pub started_at: SystemTime,
}

#[derive(Clone, Default)]
pub struct AsyncTasks {
    tasks: Arc<DashMap<u64, TaskInfo>>,
    counter: Arc<AtomicU64>,
}

impl AsyncTasks {
    pub fn get() -> Self {
        TASKS.clone()
    }

    /// Perform cutover.
    pub fn cutover() -> Result<(), Error> {
        let this = Self::get();
        let task = this
            .tasks
            .iter()
            .find(|t| t.task_kind == TaskKind::Replication)
            .ok_or(Error::NotReplication)?;

        task.cutover.notify_one();

        Ok(())
    }

    pub fn insert(task: TaskType) -> u64 {
        let this = Self::get();
        let id = this.counter.fetch_add(1, Ordering::SeqCst);
        let (abort_tx, abort_rx) = oneshot::channel();

        match task {
            TaskType::SchemaSync(handle) => {
                this.tasks.insert(
                    id,
                    TaskInfo {
                        abort_tx,
                        cutover: Arc::new(Notify::new()),
                        task_kind: TaskKind::SchemaSync,
                        started_at: SystemTime::now(),
                    },
                );
                let abort_handle = handle.abort_handle();
                spawn(async move {
                    select! {
                        _ = abort_rx => {
                            abort_handle.abort();
                        }
                        result = handle => {
                            match result {
                                Ok(Ok(())) => {}
                                Ok(Err(err)) => error!("[task: {}] {}", id, err),
                                Err(err) => error!("[task: {}] {}", id, err),
                            }
                        }
                    }
                    AsyncTasks::get().tasks.remove(&id);
                });
            }

            TaskType::CopyData(handle) => {
                this.tasks.insert(
                    id,
                    TaskInfo {
                        abort_tx,
                        cutover: Arc::new(Notify::new()),
                        task_kind: TaskKind::CopyData,
                        started_at: SystemTime::now(),
                    },
                );
                let abort_handle = handle.abort_handle();
                spawn(async move {
                    select! {
                        _ = abort_rx => {
                            abort_handle.abort();
                        }
                        result = handle => {
                            match result {
                                Ok(Ok(())) => {}
                                Ok(Err(err)) => error!("[task: {}] {}", id, err),
                                Err(err) => error!("[task: {}] {}", id, err),
                            }
                        }
                    }
                    AsyncTasks::get().tasks.remove(&id);
                });
            }

            TaskType::Replication(mut waiter) => {
                let cutover = Arc::new(Notify::new());

                this.tasks.insert(
                    id,
                    TaskInfo {
                        abort_tx,
                        cutover: cutover.clone(),
                        task_kind: TaskKind::Replication,
                        started_at: SystemTime::now(),
                    },
                );

                spawn(async move {
                    select! {
                        _ = abort_rx => {
                            waiter.stop();
                        }

                        _ = cutover.notified() => {
                            if let Err(err) = waiter.cutover().await {
                                error!("[task: {}] {}", id, err);
                            }
                        }

                        result = waiter.wait() => {
                            if let Err(err) = result {
                                error!("[task: {}] {}", id, err);
                            }
                        }
                    }

                    AsyncTasks::get().tasks.remove(&id);
                });
            }
        }

        id
    }

    pub fn remove(id: u64) -> Option<TaskKind> {
        // Dropping the sender signals abort to the waiting task
        Self::get()
            .tasks
            .remove(&id)
            .map(|(_, info)| info.task_kind)
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, TaskKind, SystemTime)> + '_ {
        self.tasks
            .iter()
            .map(|e| (*e.key(), e.value().task_kind, e.value().started_at))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    #[test]
    fn test_task_kind_display() {
        assert_eq!(TaskKind::SchemaSync.to_string(), "schema_sync");
        assert_eq!(TaskKind::CopyData.to_string(), "copy_data");
        assert_eq!(TaskKind::Replication.to_string(), "replication");
    }

    #[tokio::test]
    async fn test_task_registration_and_removal() {
        // Create a task that completes immediately
        let handle = spawn(async { Ok::<(), Error>(()) });
        let id = Task::register(TaskType::SchemaSync(handle));

        // Task should be visible briefly
        // Give it a moment to register
        sleep(Duration::from_millis(10)).await;

        // Try to remove it - it may already be gone if it completed
        let result = AsyncTasks::remove(id);
        // Either we removed it, or it already completed and removed itself
        assert!(result.is_none() || result == Some(TaskKind::SchemaSync));
    }

    #[tokio::test]
    async fn test_task_abort_via_remove() {
        // Create a long-running task
        let handle = spawn(async {
            sleep(Duration::from_secs(60)).await;
            Ok::<(), Error>(())
        });
        let id = Task::register(TaskType::CopyData(handle));

        // Give it time to register
        sleep(Duration::from_millis(10)).await;

        // Remove should abort the task
        let result = AsyncTasks::remove(id);
        assert_eq!(result, Some(TaskKind::CopyData));

        // Task should be gone now
        sleep(Duration::from_millis(50)).await;
        let result = AsyncTasks::remove(id);
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_task_iter() {
        // Create multiple tasks
        let handle1 = spawn(async {
            sleep(Duration::from_secs(60)).await;
            Ok::<(), Error>(())
        });
        let handle2 = spawn(async {
            sleep(Duration::from_secs(60)).await;
            Ok::<(), Error>(())
        });

        let id1 = Task::register(TaskType::SchemaSync(handle1));
        let id2 = Task::register(TaskType::CopyData(handle2));

        sleep(Duration::from_millis(10)).await;

        // Should see both tasks
        let tasks: Vec<_> = AsyncTasks::get().iter().collect();
        let task_ids: Vec<_> = tasks.iter().map(|(id, _, _)| *id).collect();
        assert!(task_ids.contains(&id1));
        assert!(task_ids.contains(&id2));

        // Verify task kinds
        for (id, kind, _) in &tasks {
            if *id == id1 {
                assert_eq!(*kind, TaskKind::SchemaSync);
            } else if *id == id2 {
                assert_eq!(*kind, TaskKind::CopyData);
            }
        }

        // Cleanup
        AsyncTasks::remove(id1);
        AsyncTasks::remove(id2);
    }

    #[tokio::test]
    async fn test_task_auto_cleanup_on_completion() {
        // Create a task that completes quickly
        let handle = spawn(async {
            sleep(Duration::from_millis(10)).await;
            Ok::<(), Error>(())
        });
        let id = Task::register(TaskType::SchemaSync(handle));

        // Wait for task to complete and cleanup
        sleep(Duration::from_millis(100)).await;

        // Task should have removed itself
        let result = AsyncTasks::remove(id);
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_cutover_fails_without_replication_task() {
        // Create a non-replication task
        let handle = spawn(async {
            sleep(Duration::from_secs(60)).await;
            Ok::<(), Error>(())
        });
        let id = Task::register(TaskType::SchemaSync(handle));
        sleep(Duration::from_millis(10)).await;

        // Cutover should fail because there's no replication task
        let result = AsyncTasks::cutover();
        assert!(matches!(result, Err(Error::NotReplication)), "{:?}", result);

        // Cleanup
        AsyncTasks::remove(id);
    }

    #[tokio::test]
    async fn test_cutover_returns_not_found_when_no_replication_task() {
        // Register several non-replication tasks
        let mut task_ids = vec![];
        for _ in 0..5 {
            let handle = spawn(async {
                sleep(Duration::from_secs(60)).await;
                Ok::<(), Error>(())
            });
            task_ids.push(Task::register(TaskType::SchemaSync(handle)));
        }

        sleep(Duration::from_millis(10)).await;

        // With only non-replication tasks, cutover should return TaskNotFound
        let result = AsyncTasks::cutover();
        assert!(matches!(result, Err(Error::NotReplication)));

        // Cleanup
        for id in task_ids {
            AsyncTasks::remove(id);
        }
    }
}
