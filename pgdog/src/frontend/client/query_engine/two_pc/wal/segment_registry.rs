//! Global segment registry used by the checkpointer
//! to know if it can clean up segments.

use std::{sync::Arc, time::SystemTime};

use dashmap::DashMap;
use once_cell::sync::Lazy;

/// Segment state.
#[derive(Debug, Clone, PartialEq, Copy)]
pub(crate) enum SegmentStatus {
    // Don't touch me, I'm being written to by clients.
    Active,
    // In the process of being swapped out (flushing to disk too).
    ShuttingDown,
    // Good to go, checkpoint me.
    Inactive,
}

/// Segment state with metadata.
#[derive(Debug, Clone, PartialEq, Copy)]
pub(crate) struct SegmentState {
    pub(crate) status: SegmentStatus,
    pub(crate) updated_at: SystemTime,
}

type SegmentId = u64;

static REGISTRY: Lazy<SegmentRegistry> = Lazy::new(SegmentRegistry::default);

/// The segment registry, keyed by segment ID.
#[derive(Default, Debug)]
pub(crate) struct SegmentRegistry {
    segments: Arc<DashMap<SegmentId, SegmentState>>,
}

impl SegmentRegistry {
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.segments.len()
    }

    /// Record segment status in the registry.
    pub(crate) fn record(&self, segment: SegmentId, status: SegmentStatus) {
        let updated_at = SystemTime::now();
        self.segments
            .entry(segment)
            .and_modify(|existing| {
                existing.status = status;
                existing.updated_at = updated_at;
            })
            .or_insert(SegmentState { status, updated_at });
    }

    /// Get a list of inactive segments the checkpointe can
    /// safely attempt to delete.
    pub(crate) fn inactive(&self) -> Vec<SegmentId> {
        self.segments
            .iter()
            .filter(|k| k.value().status == SegmentStatus::Inactive)
            .map(|k| *k.key())
            .collect()
    }

    /// Remove segment from registry. The caller probably deleted
    /// the file from disk, too.
    pub(crate) fn remove(&self, segment: SegmentId) {
        self.segments.remove(&segment);
    }

    /// Global accessor.
    ///
    /// TODO(lev): I should really scope this the [`super::super::Manager`] somehow.
    /// Globals are lame.
    pub(crate) fn get() -> &'static Self {
        &REGISTRY
    }
}
