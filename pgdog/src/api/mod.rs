//! PgDog API handlers.
//!
//! The interfaces that calls the api:
//! - pgdog CLI
//! - admin db api

use std::sync::LazyLock;

use crate::backend::replication::logical::Error;
use task::{TaskError, TaskStorage, TaskWaiter};

pub mod copy_data;
pub mod replication;
pub mod resharding;
pub mod schema_sync;
pub mod task;

/// Process-global task registry shared by all `crate::api` task modules.
static TASKS: LazyLock<TaskStorage> = LazyLock::new(TaskStorage::default);

/// Accessor for the process-global task registry.
pub(crate) fn tasks_storage() -> &'static TaskStorage {
    &TASKS
}

/// A composable background task: implement [`Task`] (see
/// [`task`]) to define one, then launch it as a top-level task
/// with [`start`] or nested under a running task through its
/// [`TaskContext`](task::TaskContext).
pub(crate) use task::Task;

/// Launch `task` as a top-level task in the global registry.
pub(crate) fn run_task<T: Task + 'static>(task: T) -> TaskWaiter<T::Output, T::Error> {
    tasks_storage().run(task)
}

/// Error returned by the API migration tasks: either an error from the
/// replication/orchestrator machinery, or a child task's [`TaskError`]
/// (failure, cancellation, panic, or abandonment) surfaced to its parent.
#[derive(Debug, Display, Error, From)]
pub(crate) enum MigrationError {
    Replication(Error),
    Task(TaskError<Error>),
}
