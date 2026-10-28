//! Process-wide background task tracking.

use dashmap::DashMap;
use once_cell::sync::Lazy;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

use crate::config::config;
use crate::util::safe_timeout;

static TASKS: Lazy<BackgroundTasks> = Lazy::new(BackgroundTasks::default);

#[derive(Debug, Default)]
struct BackgroundTasks {
    tracker: TaskTracker,
    shutdown: CancellationToken,
    counter: DashMap<&'static str, usize>,
}

/// Spawn a process background task that must finish before runtime teardown.
pub(crate) fn spawn<F>(name: &'static str, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    *TASKS.counter.entry(name).or_insert(0) += 1;
    let guard = CounterGuard(name);

    TASKS.tracker.spawn(async move {
        let _guard = guard;
        future.await
    })
}

struct CounterGuard(&'static str);

impl Drop for CounterGuard {
    fn drop(&mut self) {
        if let Some(mut counter) = TASKS.counter.get_mut(self.0) {
            *counter = counter.saturating_sub(1);
        }
        TASKS.counter.remove_if(self.0, |_, count| *count == 0);
    }
}

/// Shared shutdown signal for background tasks that are not tied to a pool/client signal.
pub(crate) fn shutdown_signal() -> CancellationToken {
    TASKS.shutdown.clone()
}

/// Ask all tracked background tasks to stop and wait for them.
pub(crate) async fn shutdown() {
    TASKS.shutdown.cancel();
    TASKS.tracker.close();

    info!("waiting on {} background tasks", TASKS.tracker.len());

    let wait = safe_timeout(
        config().config.general.shutdown_timeout(),
        TASKS.tracker.wait(),
    );

    if wait.await.is_err() {
        error!("unterminated background tasks: {:?}", TASKS.counter);
    }
}
