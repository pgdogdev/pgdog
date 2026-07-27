use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use super::Record;
use crate::{
    net::{Error, ToBytes},
    tasks,
};
use once_cell::sync::Lazy;
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    select,
    sync::{Mutex, Notify},
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

static SEGMENT_COUNTER: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

#[derive(Debug)]
struct LiveSegmentRecord {
    record: Record,
    flushed: CancellationToken,
}

#[derive(Debug, Clone)]
pub(super) struct LiveSegment {
    counter: u64,
    queue: Arc<Mutex<VecDeque<LiveSegmentRecord>>>,
    notify: Arc<Notify>,
    shutdown: CancellationToken,
    path: PathBuf,
}

impl LiveSegment {
    pub(super) async fn new(path: &Path) -> Result<Self, Error> {
        let number = SEGMENT_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
        let filename = path.join(number.to_string());

        let mut file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(&filename)
            .await?;
        file.write_all(&number.to_be_bytes()).await?;
        file.flush().await?;

        let segment = Self {
            counter: number,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            notify: Arc::new(Notify::new()),
            shutdown: CancellationToken::new(),
            path: filename,
        };

        let write = segment.clone();

        tasks::spawn("live segment writer", async move {
            write.writer(file).await;
        });

        Ok(segment)
    }

    /// Write a record to the WAL and wait for it
    /// to be flushed to disk.
    pub(super) async fn add(&self, record: Record) {
        let record = LiveSegmentRecord {
            record,
            flushed: CancellationToken::new(),
        };

        let flushed = record.flushed.clone();

        self.queue.lock().await.push_back(record);
        self.notify.notify_one(); // Tell the writer that we are waiting for it to write the record.

        flushed.cancelled().await
    }

    async fn writer(self, mut file: File) {
        loop {
            select! {
                _ = self.notify.notified() => {
                    self.write_records(&mut file).await.expect("this should panic and crash pgdog");
                }

                _ = self.shutdown.cancelled() => {
                    self.write_records(&mut file).await.expect("this should panic and crash pgdog");
                    break;
                }
            }
        }
    }

    async fn write_records(&self, file: &mut File) -> Result<(), Error> {
        let records: Vec<_> = self.queue.lock().await.drain(..).into_iter().collect();

        for record in &records {
            let bytes = record.record.to_bytes();
            file.write_all(&bytes).await?;
        }

        file.flush().await?;
        file.sync_all().await?; // Call fsync.

        debug!(
            "[wal] wrote {} records to \"{}\"",
            records.len(),
            self.path.display()
        );

        // Tell clients we flushed their records
        // to disk.
        for record in records {
            record.flushed.cancel();
        }

        Ok(())
    }
}
