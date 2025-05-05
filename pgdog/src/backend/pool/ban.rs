//! Pool ban.
use std::time::Duration;
use tokio::time::Instant;

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

impl std::fmt::Display for Ban {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:.3}ms)", self.reason, self.ban_timeout.as_millis())
    }
}

impl Ban {
    /// Check if the ban has expired.
    pub(super) fn expired(&self, now: Instant) -> bool {
        if self.reason == Error::ManualBan {
            false
        } else {
            let duration = now.duration_since(self.created_at);

            duration > self.ban_timeout
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
