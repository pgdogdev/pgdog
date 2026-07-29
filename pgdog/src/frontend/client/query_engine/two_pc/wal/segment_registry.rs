use std::{sync::Arc, time::SystemTime};

use dashmap::DashMap;
use once_cell::sync::Lazy;

#[derive(Debug, Clone, PartialEq, Copy)]
pub(crate) enum SegmentStatus {
    Active,
    ShuttingDown,
    Inactive,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub(crate) struct SegmentState {
    pub(crate) status: SegmentStatus,
    pub(crate) updated_at: SystemTime,
}

type SegmentId = u64;

static REGISTRY: Lazy<SegmentRegistry> = Lazy::new(SegmentRegistry::default);

#[derive(Default, Debug)]
pub(crate) struct SegmentRegistry {
    segments: Arc<DashMap<SegmentId, SegmentState>>,
}

impl SegmentRegistry {
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

    pub(crate) fn inactive(&self) -> Vec<SegmentId> {
        self.segments
            .iter()
            .filter(|k| k.value().status == SegmentStatus::Inactive)
            .map(|k| k.key().clone())
            .collect()
    }

    pub(crate) fn remove(&self, segment: SegmentId) {
        self.segments.remove(&segment);
    }

    pub(crate) fn get() -> &'static Self {
        &REGISTRY
    }
}
