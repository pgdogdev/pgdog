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
    pub fn unban(&self, manual_check: bool) {
        let mut guard = self.inner.upgradable_read();
        if let Some(ref ban) = guard.ban {
            if ban.error != Error::ManualBan || !manual_check {
                guard.with_upgraded(|guard| {
                    guard.ban = None;
                });
            }
            warn!("resuming read queries [{}]", self.pool.addr());
        }
    }

    /// Get reference to the connection pool.
    pub fn pool(&self) -> &Pool {
        &self.pool
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn test_new_ban_is_not_banned() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);
        assert!(!ban.banned());
        assert!(ban.error().is_none());
    }

    #[test]
    fn test_ban_sets_banned_state() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);
        let result = ban.ban(Error::ServerError, Duration::from_secs(1));
        assert!(result);
        assert!(ban.banned());
    }

    #[test]
    fn test_ban_returns_error() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);
        ban.ban(Error::ConnectTimeout, Duration::from_secs(1));
        assert_eq!(ban.error(), Some(Error::ConnectTimeout));
    }

    #[test]
    fn test_ban_twice_returns_false() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);
        let first = ban.ban(Error::ServerError, Duration::from_secs(1));
        let second = ban.ban(Error::ConnectTimeout, Duration::from_secs(1));
        assert!(first);
        assert!(!second);
        assert_eq!(ban.error(), Some(Error::ServerError));
    }

    #[test]
    fn test_unban_clears_state() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);
        ban.ban(Error::ServerError, Duration::from_secs(1));
        ban.unban(false);
        assert!(!ban.banned());
        assert!(ban.error().is_none());
    }

    #[test]
    fn test_unban_manual_check_does_not_clear_manual_ban() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);
        ban.ban(Error::ManualBan, Duration::from_secs(1));
        ban.unban(true);
        assert!(ban.banned());
        assert_eq!(ban.error(), Some(Error::ManualBan));
    }

    #[test]
    fn test_unban_manual_check_clears_non_manual_ban() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);
        ban.ban(Error::ServerError, Duration::from_secs(1));
        ban.unban(true);
        assert!(!ban.banned());
        assert!(ban.error().is_none());
    }

    #[test]
    fn test_unban_if_expired_before_expiration() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);
        let now = Instant::now();
        ban.ban(Error::ServerError, Duration::from_secs(10));
        let unbanned = ban.unban_if_expired(now);
        assert!(!unbanned);
        assert!(ban.banned());
    }

    #[test]
    fn test_unban_if_expired_after_expiration() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);
        let now = Instant::now();
        ban.ban(Error::ServerError, Duration::from_millis(1));
        let future = now + Duration::from_millis(10);
        let unbanned = ban.unban_if_expired(future);
        assert!(unbanned);
        assert!(!ban.banned());
    }

    #[test]
    fn test_unban_if_expired_not_banned() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);
        let now = Instant::now();
        let unbanned = ban.unban_if_expired(now);
        assert!(!unbanned);
        assert!(!ban.banned());
    }

    #[test]
    fn test_pool_reference() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);
        assert_eq!(ban.pool().id(), pool.id());
    }

    #[test]
    fn test_ban_race_condition_multiple_threads() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);
        let ban_clone1 = ban.clone();
        let ban_clone2 = ban.clone();
        let ban_clone3 = ban.clone();

        let success_count = Arc::new(AtomicUsize::new(0));
        let success1 = success_count.clone();
        let success2 = success_count.clone();
        let success3 = success_count.clone();

        let h1 = thread::spawn(move || {
            if ban_clone1.ban(Error::ServerError, Duration::from_secs(1)) {
                success1.fetch_add(1, Ordering::SeqCst);
            }
        });

        let h2 = thread::spawn(move || {
            if ban_clone2.ban(Error::ConnectTimeout, Duration::from_secs(1)) {
                success2.fetch_add(1, Ordering::SeqCst);
            }
        });

        let h3 = thread::spawn(move || {
            if ban_clone3.ban(Error::CheckoutTimeout, Duration::from_secs(1)) {
                success3.fetch_add(1, Ordering::SeqCst);
            }
        });

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();

        assert_eq!(success_count.load(Ordering::SeqCst), 1);
        assert!(ban.banned());
    }

    #[test]
    fn test_concurrent_ban_and_check() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);
        let ban_clone = ban.clone();

        let h1 = thread::spawn(move || {
            for _ in 0..1000 {
                let _ = ban_clone.banned();
            }
        });

        for _ in 0..100 {
            ban.ban(Error::ServerError, Duration::from_secs(1));
            ban.unban(false);
        }

        h1.join().unwrap();
    }

    #[test]
    fn test_concurrent_unban_if_expired() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);
        ban.ban(Error::ServerError, Duration::from_millis(1));
        thread::sleep(Duration::from_millis(10));

        let ban_clone1 = ban.clone();
        let ban_clone2 = ban.clone();
        let ban_clone3 = ban.clone();

        let unban_count = Arc::new(AtomicUsize::new(0));
        let count1 = unban_count.clone();
        let count2 = unban_count.clone();
        let count3 = unban_count.clone();

        let h1 = thread::spawn(move || {
            let now = Instant::now();
            if ban_clone1.unban_if_expired(now) {
                count1.fetch_add(1, Ordering::SeqCst);
            }
        });

        let h2 = thread::spawn(move || {
            let now = Instant::now();
            if ban_clone2.unban_if_expired(now) {
                count2.fetch_add(1, Ordering::SeqCst);
            }
        });

        let h3 = thread::spawn(move || {
            let now = Instant::now();
            if ban_clone3.unban_if_expired(now) {
                count3.fetch_add(1, Ordering::SeqCst);
            }
        });

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();

        assert_eq!(unban_count.load(Ordering::SeqCst), 1);
        assert!(!ban.banned());
    }

    #[test]
    fn test_ban_preserves_first_error() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);

        ban.ban(Error::ServerError, Duration::from_secs(1));
        ban.ban(Error::ConnectTimeout, Duration::from_secs(2));
        ban.ban(Error::CheckoutTimeout, Duration::from_secs(3));

        assert_eq!(ban.error(), Some(Error::ServerError));
    }

    #[test]
    fn test_multiple_ban_unban_cycles() {
        let pool = Pool::new_test();
        let ban = Ban::new(&pool);

        for _ in 0..10 {
            assert!(ban.ban(Error::ServerError, Duration::from_secs(1)));
            assert!(ban.banned());
            ban.unban(false);
            assert!(!ban.banned());
        }
    }
}
