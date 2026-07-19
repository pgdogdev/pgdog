//! Process-wide background task tracking.

use std::{
    future::Future,
    sync::atomic::{AtomicBool, Ordering},
};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

static TASKS: Lazy<BackgroundTasks> = Lazy::new(BackgroundTasks::default);

#[derive(Debug, Default)]
struct BackgroundTasks {
    tracker: TaskTracker,
    shutdown: CancellationToken,
    shutting_down: AtomicBool,
    counter: DashMap<&'static str, usize>,
}

/// Spawn a process background task that must finish before runtime teardown.
pub fn spawn<F>(name: &'static str, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let mut counter = TASKS.counter.entry(name).or_insert(0);
    *counter += 1;

    TASKS.tracker.spawn(async move {
        let res = future.await;
        let mut remove = false;

        if let Some(mut counter) = TASKS.counter.get_mut(name) {
            *counter = counter.saturating_sub(1);
            remove = *counter == 0;
        }

        if remove {
            TASKS.counter.remove(name);
        }

        res
    })
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

    info!("waiting on {} background tasks", TASKS.tracker.len());
    TASKS.tracker.wait().await;
}
