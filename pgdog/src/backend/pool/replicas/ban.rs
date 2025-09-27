use super::*;
use parking_lot::RwLock;
use std::time::Instant;

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
        self.inner.write().ban = None;
        self.pool.clear_server_error();
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Ban the database for the ban_timeout duration.
    pub fn ban(&self, error: Error, ban_timeout: Duration) -> bool {
        let created_at = Instant::now();
        let mut guard = self.inner.write();

        if guard.ban.is_none() {
            guard.ban = Some(BanEntry {
                created_at,
                error,
                ban_timeout,
            });
            let mut guard = self.pool.lock();
            guard.server_error = true;
            guard.dump_idle();
            true
        } else {
            false
        }
    }

    /// Remove ban if it has expired.
    pub(super) fn unban_if_expired(&self, now: Instant) -> bool {
        let mut guard = self.inner.upgradable_read();
        if guard.ban.as_ref().map(|b| b.expired(now)).unwrap_or(false) {
            guard.with_upgraded(|guard| {
                guard.ban = None;
            });

            self.pool.clear_server_error();
            true
        } else {
            false
        }
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
