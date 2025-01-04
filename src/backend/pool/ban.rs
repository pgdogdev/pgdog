//! Pool ban.
use std::time::{Duration, Instant};

use super::Error;

/// Pool ban.
#[derive(Debug)]
pub(super) struct Ban {
    /// When the banw as created.
    pub(super) created_at: Instant,
    /// Why it was created.
    pub(super) reason: Error,
}

impl Ban {
    /// Check if the ban has expired.
    pub(super) fn expired(&self, now: Instant) -> bool {
        if self.reason == Error::ManualBan {
            false
        } else {
            now.duration_since(self.created_at) > Duration::from_secs(300)
        }
    }
}
