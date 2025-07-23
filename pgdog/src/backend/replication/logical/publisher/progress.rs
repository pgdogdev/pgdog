use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::sleep;
use tokio::{select, spawn};
use tracing::info;

use crate::backend::replication::publisher::PublicationTable;

#[derive(Debug)]
struct Inner {
    table: PublicationTable,
    bytes_sharded: AtomicUsize,
    done: Notify,
}

#[derive(Debug, Clone)]
pub struct Progress {
    inner: Arc<Inner>,
}

impl Progress {
    pub fn new(table: &PublicationTable) -> Self {
        let inner = Arc::new(Inner {
            bytes_sharded: AtomicUsize::new(0),
            done: Notify::new(),
            table: table.clone(),
        });

        let notify = inner.clone();

        spawn(async move {
            let mut prev = 0;
            loop {
                select! {
                    _ = sleep(Duration::from_secs(5)) => {
                        let written = notify.bytes_sharded.load(Ordering::Relaxed);

                        info!(
                            "synced {:.3} MB for table \"{}\".\"{}\" [{:.3} MB/sec]",
                            written as f64 / 1024.0 / 1024.0,
                            notify.table.schema,
                            notify.table.name,
                            (written - prev) as f64 / 5.0 / 1024.0 / 1024.0
                        );

                        prev = written;
                    }

                    _ = notify.done.notified() => {
                        break;
                    }
                }
            }
        });

        Progress { inner }
    }

    pub fn update(&self, total_bytes: usize) {
        self.inner
            .bytes_sharded
            .store(total_bytes, Ordering::Relaxed);
    }

    pub fn done(&self) {
        self.inner.done.notify_one();
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.done()
    }
}
