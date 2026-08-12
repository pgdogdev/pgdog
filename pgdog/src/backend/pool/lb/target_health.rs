//! Keep a record of each pool's health.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Clone, Debug)]
pub(crate) struct TargetHealth {
    pub(super) healthy: Arc<AtomicBool>,
}

impl TargetHealth {
    pub(crate) fn new() -> Self {
        Self {
            healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    pub(crate) fn toggle(&self, healthy: bool) {
        self.healthy.swap(healthy, Ordering::SeqCst);
    }

    pub(crate) fn healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }
}
