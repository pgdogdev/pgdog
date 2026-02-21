use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::SystemTime,
};

use crate::backend::replication::Waiter;

use super::Error;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use tokio::{select, spawn, sync::oneshot, task::JoinHandle};
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
    Replication(Waiter),
}

pub struct TaskInfo {
    #[allow(dead_code)]
    abort_tx: oneshot::Sender<()>,
    pub task_type: &'static str,
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
                        task_type: "schema_sync",
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
                        task_type: "copy_data",
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
                this.tasks.insert(
                    id,
                    TaskInfo {
                        abort_tx,
                        task_type: "replication",
                        started_at: SystemTime::now(),
                    },
                );
                spawn(async move {
                    select! {
                        _ = abort_rx => {
                            waiter.stop();
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

    pub fn remove(id: u64) -> Option<&'static str> {
        // Dropping the sender signals abort to the waiting task
        Self::get()
            .tasks
            .remove(&id)
            .map(|(_, info)| info.task_type)
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, &'static str, SystemTime)> + '_ {
        self.tasks
            .iter()
            .map(|e| (*e.key(), e.value().task_type, e.value().started_at))
    }
}
