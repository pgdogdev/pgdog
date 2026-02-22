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
    Replication(ReplicationWaiter),
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
        let task = this.tasks.iter().next().ok_or(Error::TaskNotFound)?;

        if task.task_kind != TaskKind::Replication {
            return Err(Error::NotReplication);
        }

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
