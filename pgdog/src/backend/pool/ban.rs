//! Pool ban.
use std::time::{Duration, Instant};

use super::Error;

/// Pool ban.
#[derive(Debug, Copy, Clone)]
pub struct Ban {
    /// When the banw as created.
    pub(super) created_at: Instant,
    /// Why it was created.
    pub(super) reason: Error,
    /// Ban timeout
    pub(super) ban_timeout: Duration,
}

impl Ban {
    /// Check if the ban has expired.
    pub(super) fn expired(&self, now: Instant) -> bool {
        if self.reason == Error::ManualBan {
            false
        } else {
            now.duration_since(self.created_at) > self.ban_timeout
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_expired() {
        let ban_timeout = Duration::from_secs(300);
        let created_at = Instant::now();

        let mut ban = Ban {
            created_at,
            reason: Error::CheckoutTimeout,
            ban_timeout,
        };

        let later = created_at + ban_timeout + Duration::from_secs(1);

        assert!(!ban.expired(Instant::now()));
        assert!(ban.expired(later));

        ban.reason = Error::ManualBan;
        assert!(!ban.expired(later));
    }
}
