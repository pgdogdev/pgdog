use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{Record, Segment, SegmentRegistry, SegmentStatus};
use crate::{
    net::{Error, ToBytes},
    tasks,
};
use parking_lot::Mutex;
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    select,
    sync::Notify,
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

// WAL data format version.
const VERSION: u32 = 0;
// WAL filename extension
pub(super) const EXTENSION: &str = "2pc";

#[derive(Debug)]
struct LiveSegmentRecord {
    record: Record,
    flushed: CancellationToken,
}

#[derive(Debug, Default)]
struct Inner {
    queue: VecDeque<LiveSegmentRecord>,
    written: usize,
}

#[derive(Debug, Clone)]
pub(super) struct Waiter {
    pub(super) segment_size: usize,
    pub(super) waiter: CancellationToken,
}

impl Waiter {
    pub(super) async fn wait_flush(&self) {
        self.waiter.cancelled().await
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LiveSegment {
    queue: Arc<Mutex<Inner>>,
    writer_signal: Arc<Notify>,
    shutdown: CancellationToken,
    path: PathBuf,
    counter: u64,
}

impl LiveSegment {
    /// Create new segment in the WAL directory with the
    /// given sequence number.
    pub(super) async fn new(wal_directory: &Path, number: u64) -> Result<Self, Error> {
        let filename = Segment::path(wal_directory, number);

        debug!(
            r#"[2pc] creating new WAL segment at "{}""#,
            filename.display()
        );

        let mut file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(&filename)
            .await?;
        file.write_all(&number.to_be_bytes()).await?;
        file.write_all(&VERSION.to_be_bytes()).await?;
        file.flush().await?;

        let segment = Self {
            queue: Arc::new(Mutex::new(Inner::default())),
            writer_signal: Arc::new(Notify::new()),
            shutdown: CancellationToken::new(),
            path: filename,
            counter: number,
        };

        let write = segment.clone();

        tasks::spawn("live segment writer", async move {
            write.writer(file).await;
        });

        Ok(segment)
    }

    /// Write a record to the WAL and wait for it
    /// to be flushed to disk.
    #[must_use]
    pub(super) fn add(&self, record: Record) -> Waiter {
        let len = record.len();

        let record = LiveSegmentRecord {
            record,
            flushed: CancellationToken::new(),
        };

        let waiter = record.flushed.clone();

        let segment_size = {
            let mut guard = self.queue.lock();
            guard.queue.push_back(record);
            guard.written += len;
            guard.written
        };

        self.writer_signal.notify_one();

        Waiter {
            segment_size,
            waiter,
        }
    }

    /// Flush all writes to this segment and stop
    /// the writer.
    pub(crate) fn shutdown(&self) {
        self.shutdown.cancel();
        SegmentRegistry::get().record(self.counter, SegmentStatus::ShuttingDown);
    }

    async fn writer(self, mut file: File) {
        loop {
            select! {
                _ = self.writer_signal.notified() => {
                    self.write_records(&mut file).await.expect("this should panic and crash pgdog");
                }

                _ = self.shutdown.cancelled() => {
                    self.write_records(&mut file).await.expect("this should panic and crash pgdog");
                    break;
                }
            }
        }

        // Mark this segment ready for cleanup.
        SegmentRegistry::get().record(self.counter, SegmentStatus::Inactive);
    }

    async fn write_records(&self, file: &mut File) -> Result<(), Error> {
        let records: Vec<_> = self.queue.lock().queue.drain(..).into_iter().collect();

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
