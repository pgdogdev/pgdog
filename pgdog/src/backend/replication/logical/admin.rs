use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use crate::backend::replication::Waiter;

use super::Error;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use tokio::{spawn, task::JoinHandle};
use tracing::error;

static TASKS: Lazy<AsyncTasks> = Lazy::new(AsyncTasks::default);

#[derive(Debug)]
pub struct Task {
    task: Option<TaskType>,
}

impl Drop for Task {
    fn drop(&mut self) {
        self.task.take().map(|task| task.abort());
    }
}

impl Task {
    pub(crate) fn register(task: TaskType) -> u64 {
        AsyncTasks::insert(task)
    }

    pub(crate) fn abort(task: u64) {
        AsyncTasks::remove(task);
    }
}

#[derive(Debug)]
pub enum TaskType {
    SchemaSync(JoinHandle<Result<(), Error>>),
    CopyData(JoinHandle<Result<(), Error>>),
    Replication(Waiter),
}

impl TaskType {
    fn abort(self) {
        match self {
            Self::SchemaSync(handle) => {
                if !handle.is_finished() {
                    handle.abort()
                }
            }
            Self::CopyData(handle) => {
                if !handle.is_finished() {
                    handle.abort()
                }
            }
            Self::Replication(mut waiter) => {
                spawn(async move {
                    if let Err(err) = waiter.wait().await {
                        error!("replication task finished with error: {}", err);
                    }
                });
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct AsyncTasks {
    tasks: Arc<DashMap<u64, TaskType>>,
    counter: Arc<AtomicU64>,
}

impl AsyncTasks {
    pub fn get() -> Self {
        TASKS.clone()
    }

    pub fn insert(task: TaskType) -> u64 {
        let id = Self::get().counter.fetch_add(1, Ordering::SeqCst);
        Self::get().tasks.insert(id, task);

        id
    }

    pub fn remove(id: u64) {
        Self::get().tasks.remove(&id).map(|task| task.1.abort());
    }
}
