//! Keep a record of each pool's health.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Clone, Debug)]
pub struct TargetHealth {
    #[allow(dead_code)]
    pub(super) id: u64,
    pub(super) healthy: Arc<AtomicBool>,
}

impl TargetHealth {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn toggle(&self, healthy: bool) {
        self.healthy.swap(healthy, Ordering::SeqCst);
    }

    pub fn healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }
}
