use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::select;
use tokio::sync::Notify;
use tracing::info;

use crate::backend::replication::publisher::Lsn;
use crate::tasks;
use crate::util::safe_sleep;

#[derive(Debug)]
struct Inner {
    bytes_sharded: AtomicUsize,
    lsn: AtomicI64,
    done: Notify,
}

#[derive(Debug, Clone)]
pub(crate) struct Progress {
    inner: Arc<Inner>,
}

impl Progress {
    pub(crate) fn new_stream() -> Self {
        let inner = Arc::new(Inner {
            bytes_sharded: AtomicUsize::new(0),
            lsn: AtomicI64::new(0),
            done: Notify::new(),
        });

        let notify = inner.clone();

        tasks::spawn("logical publisher progress", async move {
            let mut prev = 0;
            loop {
                select! {
                    _ = safe_sleep(Duration::from_secs(5)) => {
                        let written = notify.bytes_sharded.load(Ordering::Relaxed);
                        let lsn = notify.lsn.load(Ordering::Relaxed);

                        info!(
                            "replicated {:.3} MB position {} [{:.3} MB/sec]",
                            written as f64 / 1024.0 / 1024.0,
                            Lsn::from_i64(lsn),
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

    pub(crate) fn update(&self, total_bytes: usize, lsn: i64) {
        self.inner
            .bytes_sharded
            .store(total_bytes, Ordering::Relaxed);
        self.inner.lsn.store(lsn, Ordering::Relaxed);
    }

    pub(crate) fn done(&self) {
        self.inner.done.notify_one();
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.done()
    }
}
