use super::*;
use parking_lot::RwLock;
use std::time::Instant;

use tracing::{error, warn};

/// Load balancer target ban.
#[derive(Clone, Debug)]
pub struct Ban {
    inner: Arc<RwLock<BanInner>>,
    pool: Pool,
}

impl Ban {
    /// Create new ban handler.
    pub(super) fn new(pool: &Pool) -> Self {
        Self {
            inner: Arc::new(RwLock::new(BanInner { ban: None })),
            pool: pool.clone(),
        }
    }

    /// Check if the database is banned.
    pub fn banned(&self) -> bool {
        self.inner.read().ban.is_some()
    }

    /// Get ban error, if any.
    pub fn error(&self) -> Option<Error> {
        self.inner.read().ban.as_ref().map(|b| b.error)
    }

    /// Unban the database.
    pub fn unban(&self) {
        let mut guard = self.inner.upgradable_read();
        if guard.ban.is_some() {
            guard.with_upgraded(|guard| {
                guard.ban = None;
            });
            warn!("resuming read queries [{}]", self.pool.addr());
        }
    }

    /// Get reference to the connection pool.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Ban pool if its reporting a server error.
    pub fn ban_if_unhealthy(&self) -> bool {
        if !self.pool.inner().health.healthy() {
            self.ban(Error::ServerError, self.pool.config().ban_timeout)
        } else {
            false
        }
    }

    /// Ban the database for the ban_timeout duration.
    pub fn ban(&self, error: Error, ban_timeout: Duration) -> bool {
        let created_at = Instant::now();
        let mut guard = self.inner.upgradable_read();

        if guard.ban.is_none() {
            guard.with_upgraded(|guard| {
                guard.ban = Some(BanEntry {
                    created_at,
                    error,
                    ban_timeout,
                });
                self.pool.lock().dump_idle();
            });
            drop(guard);
            error!("read queries banned: {} [{}]", error, self.pool.addr());
            true
        } else {
            false
        }
    }

    /// Remove ban if it has expired.
    pub(super) fn unban_if_expired(&self, now: Instant) -> bool {
        let mut guard = self.inner.upgradable_read();
        let unbanned = if guard.ban.as_ref().map(|b| b.expired(now)).unwrap_or(false) {
            guard.with_upgraded(|guard| {
                guard.ban = None;
            });

            true
        } else {
            false
        };
        drop(guard);
        if unbanned {
            warn!("resuming read queries [{}]", self.pool.addr());
        }
        unbanned
    }
}

#[derive(Debug)]
struct BanEntry {
    created_at: Instant,
    error: Error,
    ban_timeout: Duration,
}

#[derive(Debug)]
pub(super) struct BanInner {
    ban: Option<BanEntry>,
}

impl BanEntry {
    fn expired(&self, now: Instant) -> bool {
        now.duration_since(self.created_at) >= self.ban_timeout
    }
}
