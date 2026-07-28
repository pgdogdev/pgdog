use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use crate::net::Error;
use arc_swap::ArcSwap;

use super::*;

/// WAL writer.
#[derive(Debug, Clone)]
pub(crate) struct WalWriter {
    /// Current segment.
    ///
    /// We use an ArcSwap and not a Mutex
    /// to avoid blocking writers while we create
    /// a new segment during the swap.
    segment: Arc<ArcSwap<LiveSegment>>,
    path: PathBuf,
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    number: AtomicU64,
    written: AtomicUsize,
    swapping: AtomicBool,
}

impl WalWriter {
    /// Write a record to the WAL.
    pub(crate) async fn add(&self, record: impl Into<Record>) -> Result<(), Error> {
        let written = self.segment.load_full().add(record.into()).await;
        let total = self.inner.written.fetch_add(written, Ordering::Relaxed) + written;

        // This is atomic and only one
        // client can perform the swap at a time.
        //
        // If a client started a swap, other clients
        // will continue to write to the old segment.
        //
        // This is safe, the old segment will flush all records
        // written to it on shutdown.
        if total > 16_000_000 {
            let can_swap = self.inner.swapping.swap(true, Ordering::SeqCst);

            if can_swap {
                self.swap().await?;
            }
        }

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
    pub(super) async fn swap(&self) -> Result<(), Error> {
        let segment =
            LiveSegment::new(&self.path, self.inner.number.fetch_add(1, Ordering::SeqCst)).await?;
        let current = self.segment.swap(Arc::new(segment));

        current.shutdown();

        Ok(())
    }

    pub(crate) async fn new(path: &PathBuf, counter: u64) -> Result<Self, Error> {
        // Create the first segment.
        let segment = LiveSegment::new(path, counter).await?;

        Ok(Self {
            path: path.clone(),
            segment: Arc::new(ArcSwap::new(Arc::new(segment))),
            inner: Arc::new(Inner {
                number: AtomicU64::new(counter + 1),
                ..Default::default()
            }),
        })
    }
}
