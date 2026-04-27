//! Process-level 2PC counters surfaced via the OpenMetrics endpoint.
//!
//! Counters are bumped from the manager during recovery and read by
//! `pgdog::stats::two_pc::TwoPc::load`.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct TwoPcStats {
    /// Total number of in-flight 2PC transactions restored from the
    /// WAL during recovery since this pgdog process started.
    recovered_total: AtomicU64,
}

impl TwoPcStats {
    pub fn incr_recovered(&self) {
        self.recovered_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn recovered_total(&self) -> u64 {
        self.recovered_total.load(Ordering::Relaxed)
    }
}
