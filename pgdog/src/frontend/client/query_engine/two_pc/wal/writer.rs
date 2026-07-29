use std::{path::PathBuf, sync::Arc};

use crate::net::Error;
use arc_swap::ArcSwap;
use tokio::sync::Mutex;

use super::*;

type SegmentId = u64;

/// WAL writer.
#[derive(Debug, Clone)]
pub(crate) struct WalWriter {
    /// Current segment.
    segment: Arc<ArcSwap<LiveSegment>>,
    path: PathBuf,
    next_segment_id: Arc<Mutex<SegmentId>>,
    segment_size: usize,
}

impl WalWriter {
    /// Write a record to the WAL.
    pub(crate) async fn add(&self, record: impl Into<Record>) -> Result<(), Error> {
        let waiter = {
            // Grab the WAL write lock.
            let mut next_segment_id = self.next_segment_id.lock().await;
            // Add record to current WAL segment.
            let waiter = self.segment.load_full().add(record.into());
            // Check if segment needs to be swapped
            // and swap if so.
            //
            // Having the lock here is quick since most of the time,
            // the segment won't need the swap.
            if waiter.segment_size > self.segment_size {
                let segment_id = *next_segment_id;
                *next_segment_id += 1;
                self.swap(segment_id).await?;
            }
            waiter
        };

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
        let segment = LiveSegment::new(&self.path, number).await?;
        let current = self.segment.swap(Arc::new(segment));

        current.shutdown();

        Ok(())
    }

    pub(crate) async fn new(
        path: &PathBuf,
        next_segment_id: u64,
        segment_size: usize,
    ) -> Result<Self, Error> {
        // Create the first segment.
        let segment = LiveSegment::new(path, next_segment_id).await?;

        SegmentRegistry::get().record(next_segment_id, SegmentStatus::Active);

        Ok(Self {
            path: path.clone(),
            segment: Arc::new(ArcSwap::new(Arc::new(segment))),
            next_segment_id: Arc::new(Mutex::new(next_segment_id + 1)),
            segment_size,
        })
    }
}
