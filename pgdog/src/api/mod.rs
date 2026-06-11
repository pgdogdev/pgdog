//! PgDog API handlers.
//!
//! The interfaces that calls the api:
//! - pgdog CLI
//! - admin db api

use std::sync::LazyLock;

use crate::backend::replication::logical::Error;
use async_task::{AsyncTaskWaiter, AsyncTasksStorage, TaskError};

pub mod async_task;
pub mod copy_data;
pub mod replication;
pub mod resharding;
pub mod schema_sync;

/// Process-global task registry shared by all `crate::api` task modules.
static TASKS: LazyLock<AsyncTasksStorage> = LazyLock::new(AsyncTasksStorage::default);

/// Accessor for the process-global task registry.
pub(crate) fn storage() -> &'static AsyncTasksStorage {
    &TASKS
}

/// A composable background task: implement [`Task`] (see
/// [`async_task`]) to define one, then launch it as a top-level task
/// with [`start`] or nested under a running task through its
/// [`AsyncTaskContext`](async_task::AsyncTaskContext).
pub(crate) use async_task::Task;

/// Launch `task` as a top-level task in the global registry.
pub(crate) fn start<T: Task>(task: T) -> AsyncTaskWaiter<T::Output, T::Error> {
    storage().run(task)
}

/// Error returned by the API migration tasks: either an error from the
/// replication/orchestrator machinery, or a child task's [`TaskError`]
/// (failure, cancellation, panic, or abandonment) surfaced to its parent.
#[derive(Debug, Display, Error, From)]
pub(crate) enum MigrationError {
    #[display("{_0}")]
    Replication(Error),
    #[display("{_0}")]
    Task(TaskError<Error>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_error_wraps_replication_and_task_errors() {
        // A replication/orchestrator error converts directly.
        let err = MigrationError::from(Error::NoSchema);
        assert!(matches!(err, MigrationError::Replication(Error::NoSchema)));

        // A child task's failure is wrapped, preserving the inner error.
        let err = MigrationError::from(TaskError::Failed(Error::NoSchema));
        assert!(matches!(
            err,
            MigrationError::Task(TaskError::Failed(Error::NoSchema))
        ));

        // Non-failure child outcomes are preserved too (not stringified).
        let err = MigrationError::from(TaskError::<Error>::Cancelled);
        assert!(matches!(err, MigrationError::Task(TaskError::Cancelled)));
    }
}
