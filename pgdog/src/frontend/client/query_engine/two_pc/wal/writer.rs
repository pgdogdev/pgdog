use std::time::Duration;
use std::{path::PathBuf, sync::Arc};

use crate::net::Error;
use crate::tasks;
use arc_swap::{ArcSwap, ArcSwapOption};
use tokio::select;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use super::super::Manager;
use super::*;

type SegmentId = u64;

#[derive(Debug, Default)]
struct Signals {
    swap: Notify,
    shutdown: CancellationToken,
}

/// WAL writer.
#[derive(Debug, Clone)]
pub(crate) struct WalWriter {
    /// Current segment.
    segment: Arc<ArcSwap<LiveSegment>>,
    wal_directory: PathBuf,
    next_segment_id: Arc<Mutex<SegmentId>>,
    signals: Arc<Signals>,
    segment_size: usize,
    checkpointer: Arc<ArcSwapOption<Checkpointer>>,
}

impl WalWriter {
    /// Write a record to the WAL.
    pub(crate) async fn add(&self, record: impl Into<Record>) -> Result<(), Error> {
        let waiter = self.segment.load_full().add(record.into());

        if waiter.segment_size > self.segment_size {
            self.signals.swap.notify_one();
        }

        waiter.wait_flush().await;

        Ok(())
    }

    /// Create new segment and flush the current one to disk.
    /// Use the new segment for all subsequent WAL writes.
    ///
    /// This operation is atomic.
    ///
    /// # Cancellation safety
    ///
    /// This function is not cancel-safe. If cancelled,
    /// the segment it will attempt to create will be corrupted
    /// and unusable. However, clients will not be able to write to it,
    /// so it's possible to call this function again to create a new,
    /// valid segment.
    ///
    pub(super) async fn swap(&self, number: u64) -> Result<(), Error> {
        let segment = LiveSegment::new(&self.wal_directory, number).await?;
        let current = self.segment.swap(Arc::new(segment));

        SegmentRegistry::get().record(number, SegmentStatus::Active);
        current.shutdown();

        Ok(())
    }

    /// Create new WAL writer at segment position.
    pub(crate) async fn new(
        wal_directory: &PathBuf,
        next_segment_id: u64,
        segment_size: usize,
    ) -> Result<Self, Error> {
        // Create the first segment.
        let segment = LiveSegment::new(wal_directory, next_segment_id).await?;

        SegmentRegistry::get().record(next_segment_id, SegmentStatus::Active);

        let writer = Self {
            wal_directory: wal_directory.clone(),
            segment: Arc::new(ArcSwap::new(Arc::new(segment))),
            next_segment_id: Arc::new(Mutex::new(next_segment_id + 1)),
            segment_size,
            signals: Arc::new(Signals::default()),
            checkpointer: Arc::new(ArcSwapOption::empty()),
        };

        let background = writer.clone();

        tasks::spawn("wal bg writer", async move {
            background.monitor().await;
        });

        Ok(writer)
    }

    pub(crate) fn enable_checkpointer(&self, manager: Manager, checkpoint_interval: Duration) {
        let checkpointer = Checkpointer::new(&self.wal_directory, manager, checkpoint_interval);
        checkpointer.spawn();

        self.checkpointer.store(Some(Arc::new(checkpointer)));
    }

    pub(crate) fn shutdown(&self) {
        self.signals.shutdown.cancel();

        if let Some(checkpointer) = self.checkpointer.load_full() {
            checkpointer.shutdown();
        }
    }

    /// Background swapping thread.
    async fn monitor(self) {
        debug!("[2pc] writer background started");

        loop {
            select! {
                _ = self.signals.swap.notified() => {},
                _ = self.signals.shutdown.cancelled() => { break; },
            };

            let need_swap = self.segment.load().size() > self.segment_size;

            if !need_swap {
                continue;
            }

            // Hold the mutex while we swap,
            // so we can check if the swapping is taking place externally.
            let mut next_segment_id = self.next_segment_id.lock().await;
            let segment_id = *next_segment_id;
            *next_segment_id += 1;

            match self.swap(segment_id).await {
                Ok(_) => (),
                Err(err) => {
                    error!("[2pc] writer couldn't rotate WAL segments: {err}");
                }
            }
        }

        debug!("[2pc] writer background done");
    }
}
