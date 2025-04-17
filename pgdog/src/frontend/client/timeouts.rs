use std::time::Duration;

use crate::{config::General, state::State};

#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    pub(super) query_timeout: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            query_timeout: Duration::MAX,
        }
    }
}

impl Timeouts {
    pub(crate) fn from_config(general: &General) -> Self {
        Self {
            query_timeout: general.query_timeout(),
        }
    }

    /// Get active query timeout.
    #[inline]
    pub(crate) fn query_timeout(&self, state: &State) -> Duration {
        match state {
            State::Active => self.query_timeout,
            _ => Duration::MAX,
        }
    }
}
