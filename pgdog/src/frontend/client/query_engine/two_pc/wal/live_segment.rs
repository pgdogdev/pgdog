//! Reference to the currently open WAL segment.

use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use super::{Record, Segment, SegmentRegistry, SegmentStatus};
use crate::{
    net::{Error, ToBytes},
    tasks,
    util::safe_interval,
};
use parking_lot::Mutex;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
    select,
    sync::{Notify, oneshot},
    time::MissedTickBehavior,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

// WAL data format version.
// Not used, but nice to have to avoid breaking compatibility.
const VERSION: u32 = 0;
// WAL filename extension.
pub(super) const EXTENSION: &str = "2pc";

#[derive(Debug)]
struct LiveSegmentRecord {
    record: Record,
    flushed: oneshot::Sender<()>,
    needs_fsync: bool,
}

#[derive(Debug, Default)]
struct Queue {
    queue: VecDeque<LiveSegmentRecord>,
    written: usize,
}

#[derive(Debug)]
pub(super) struct Waiter {
    pub(super) segment_size: usize,
    pub(super) waiter: oneshot::Receiver<()>,
}

impl Waiter {
    /// Wait for the segment to be flushed to disk.
    pub(super) async fn wait_flush(self) {
        let _ = self.waiter.await;
    }
}

/// The currently open WAL segment.
#[derive(Debug, Clone)]
pub(crate) struct LiveSegment {
    queue: Arc<Mutex<Queue>>,
    writer_signal: Arc<Notify>,
    shutdown: CancellationToken,
    segment_path: PathBuf,
    counter: u64,
    fsync_interval: Duration,
}

impl LiveSegment {
    /// Create new segment in the WAL directory with the
    /// given sequence number.
    pub(super) async fn new(
        wal_directory: &Path,
        number: u64,
        fsync_interval: Duration,
    ) -> Result<Self, Error> {
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
        file.sync_all().await?;

        // Persist the filename as well as the file itself. fsync on a newly
        // created file does not make its parent directory entry durable.
        #[cfg(unix)]
        {
            let directory = File::open(wal_directory).await?;
            directory.sync_all().await?;
        }

        let segment = Self {
            queue: Arc::new(Mutex::new(Queue::default())),
            writer_signal: Arc::new(Notify::new()),
            shutdown: CancellationToken::new(),
            segment_path: filename,
            counter: number,
            fsync_interval,
        };

        let write = segment.clone();

        // Each segment is responsible for writing to itself.
        // No global WALWriteLock :)
        tasks::spawn("live segment writer", async move {
            write.writer(file).await;
        });

        Ok(segment)
    }

    /// Write a record to the WAL. The returned waiter
    /// should be awaited if you care that your write
    /// actually makes it to disk.
    #[must_use]
    pub(super) fn add(&self, record: Record) -> Waiter {
        let len = record.len();
        let needs_fsync = record.needs_fsync();

        let (tx, rx) = oneshot::channel();

        let record = LiveSegmentRecord {
            record,
            flushed: tx,
            needs_fsync,
        };

        let segment_size = {
            let mut guard = self.queue.lock();
            guard.queue.push_back(record);
            guard.written += len;
            guard.written
        };

        self.writer_signal.notify_one();

        Waiter {
            segment_size,
            waiter: rx,
        }
    }

    /// How big is this segment in bytes.
    pub(super) fn size(&self) -> usize {
        self.queue.lock().written
    }

    /// Flush all writes to this segment and stop
    /// the writer.
    pub(crate) fn shutdown(&self) {
        self.shutdown.cancel();
        SegmentRegistry::get().record(self.counter, SegmentStatus::ShuttingDown);
    }

    async fn writer(self, file: File) {
        let mut file = BufWriter::new(file);
        let global_shutdown = tasks::shutdown_signal();
        let mut fsync = if self.fsync_interval.is_zero() {
            None
        } else {
            let mut fsync = safe_interval(self.fsync_interval);
            fsync.set_missed_tick_behavior(MissedTickBehavior::Delay);
            fsync.tick().await; // The first tick completes immediately.
            Some(fsync)
        };

        loop {
            let exit = select! {
                _ = self.writer_signal.notified(), if fsync.is_none() => { false }
                _ = async { fsync.as_mut().unwrap().tick().await }, if fsync.is_some() => { false }
                // INVARIANT: Global shutdown is called once all clients
                // finished running transactions.
                //
                // FIXME(lev): This is a bit of a footgun, since we don't
                // have a synchronization primitive
                // that tells us no more clients
                // are writing to the segment.
                _ = global_shutdown.cancelled() => { true }
                _ = self.shutdown.cancelled() => { true }
            };

            if let Err(err) = self.write_records(&mut file).await {
                error!("[2pc] wal writer error: {err}");
            }

            if exit {
                break;
            }
        }

        // Mark this segment ready for cleanup.
        SegmentRegistry::get().record(self.counter, SegmentStatus::Inactive);
    }

    async fn write_records(&self, file: &mut BufWriter<File>) -> Result<(), Error> {
        let records: Vec<_> = self.queue.lock().queue.drain(..).collect();

        if records.is_empty() {
            return Ok(());
        }

        let record_count = records.len();

        for record in &records {
            let bytes = record.record.to_bytes();
            file.write_all(&bytes).await?;
        }

        file.flush().await?;

        // Preserve WAL order on disk, but don't make records which only clean
        // up recovery state wait for a durability barrier.
        let (needs_fsync, flushed): (Vec<_>, Vec<_>) =
            records.into_iter().partition(|record| record.needs_fsync);

        for record in flushed {
            let _ = record.flushed.send(());
        }

        if !needs_fsync.is_empty() {
            file.get_mut().sync_data().await?; // This is sufficient for recovery.
        }

        debug!(
            "[wal] wrote {} records to \"{}\"",
            record_count,
            self.segment_path.display()
        );

        // Tell clients their records are durable.
        for record in needs_fsync {
            let _ = record.flushed.send(());
        }

        Ok(())
    }
}
