use std::time::Duration;

use tokio::select;
use tracing::info;

use crate::frontend::router::parser::Cache;
use crate::tasks;
use crate::util::safe_sleep;

#[derive(Debug, Clone)]
pub struct Logger {
    interval: Duration,
}

impl Default for Logger {
    fn default() -> Self {
        Self::new()
    }
}

impl Logger {
    pub fn new() -> Self {
        Self {
            interval: Duration::from_secs(10),
        }
    }

    pub fn spawn(&self) {
        let me = self.clone();

        tasks::spawn("stats logger", async move {
            let shutdown = tasks::shutdown_signal();
            loop {
                select! {
                    _ = safe_sleep(me.interval) => {
                        let (stats, len) = Cache::stats();

                        info!(
                            "[query cache stats] direct: {}, multi: {}, hits: {}, misses: {}, size: {}, direct hit rate: {:.3}%",
                            stats.direct, stats.multi, stats.hits, stats.misses, len, (stats.direct as f64 / std::cmp::max(stats.direct + stats.multi, 1) as f64 * 100.0)
                        );
                    }
                    _ = shutdown.cancelled() => break,
                }
            }
        });
    }
}
