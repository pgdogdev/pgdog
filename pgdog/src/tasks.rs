//! Process-wide background task tracking.

use std::{
    future::Future,
    sync::atomic::{AtomicBool, Ordering},
};

use once_cell::sync::Lazy;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

static TASKS: Lazy<BackgroundTasks> = Lazy::new(BackgroundTasks::default);

#[derive(Debug, Default)]
struct BackgroundTasks {
    tracker: TaskTracker,
    shutdown: CancellationToken,
    shutting_down: AtomicBool,
}

/// Spawn a process background task that must finish before runtime teardown.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    TASKS.tracker.spawn(future)
}

/// Shared shutdown signal for background tasks that are not tied to a pool/client signal.
pub fn shutdown_signal() -> CancellationToken {
    TASKS.shutdown.clone()
}

/// True once process background tasks have been asked to stop.
pub fn shutting_down() -> bool {
    TASKS.shutting_down.load(Ordering::Relaxed)
}

/// Ask all tracked background tasks to stop and wait for them.
pub async fn shutdown() {
    TASKS.shutting_down.store(true, Ordering::Relaxed);
    TASKS.shutdown.cancel();
    TASKS.tracker.close();
    TASKS.tracker.wait().await;
}
