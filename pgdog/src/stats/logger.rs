use std::{sync::Arc, time::Duration};

use tokio::{select, spawn, sync::Notify, time::sleep};
use tracing::info;

use crate::frontend::router::parser::Cache;

#[derive(Debug, Clone)]
pub struct Logger {
    interval: Duration,
    shutdown: Arc<Notify>,
}

impl Logger {
    pub fn new() -> Self {
        Self {
            interval: Duration::from_secs(10),
            shutdown: Arc::new(Notify::new()),
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    pub fn spawn(&self) {
        let me = self.clone();

        spawn(async move {
            loop {
                select! {
                    _ = sleep(me.interval) => {
                        let stats = Cache::stats();

                        info!(
                            "[query cache stats] direct: {}, multi: {}, hits: {}, misses: {}, size: {}, direct hit rate: {:.3}%",
                            stats.direct, stats.multi, stats.hits, stats.misses, stats.size, (stats.direct as f64 / std::cmp::max(stats.direct + stats.multi, 1) as f64 * 100.0)
                        );
                    }
                    _ = me.shutdown.notified() => break,
                }
            }
        });
    }
}
