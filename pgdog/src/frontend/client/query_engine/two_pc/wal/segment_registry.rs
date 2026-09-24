//! Global segment registry used by the checkpointer
//! to know if it can clean up segments.

use std::{sync::Arc, time::SystemTime};

use dashmap::DashMap;
use fnv::{FnvHashMap as HashMap, FnvHashSet as HashSet};
use once_cell::sync::Lazy;
use parking_lot::Mutex;

use super::super::TwoPcTransaction;

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
    // Registered before a phase record is queued, including records that have
    // not reached disk yet. Checkpoint selection holds this same mutex.
    segment_to_transaction_mapping: Mutex<HashMap<SegmentId, HashSet<TwoPcTransaction>>>,
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
        self.segment_to_transaction_mapping.lock().remove(&segment);
    }

    /// Record that a particual segment was used to write information about a transaction.
    pub(super) fn add_phase_reference(&self, segment: SegmentId, transaction: TwoPcTransaction) {
        self.segment_to_transaction_mapping
            .lock()
            .entry(segment)
            .or_default()
            .insert(transaction);
    }

    /// Serialize checkpoint selection with publication of new dependencies.
    pub(super) fn with_segment_to_transaction_mapping<T>(
        &self,
        select: impl FnOnce(&HashMap<SegmentId, HashSet<TwoPcTransaction>>) -> T,
    ) -> T {
        select(&self.segment_to_transaction_mapping.lock())
    }

    /// Global accessor.
    ///
    /// TODO(lev): I should really scope this the [`super::super::Manager`] somehow.
    /// Globals are lame.
    pub(crate) fn get() -> &'static Self {
        &REGISTRY
    }
}
