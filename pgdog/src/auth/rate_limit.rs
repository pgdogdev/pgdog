//! Rate limiting for authentication attempts.

use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use lru::LruCache;
use nonzero_ext::nonzero;
use once_cell::sync::Lazy;
use std::net::IpAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use parking_lot::Mutex;

use crate::config::config;

/// Global rate limiter for authentication attempts per IP address.
pub static AUTH_RATE_LIMITER: Lazy<AuthRateLimiter> = Lazy::new(AuthRateLimiter::new);

/// Maximum number of IP addresses to track in the rate limiter cache.
/// This prevents unbounded memory growth from storing every IP that ever connects.
const MAX_TRACKED_IPS: usize = 10_000;

/// Rate limiter for authentication attempts.
pub struct AuthRateLimiter {
    limiters: Arc<Mutex<LruCache<IpAddr, Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>>>>,
    quota: Quota,
}

impl AuthRateLimiter {
    /// Create a new rate limiter.
    pub fn new() -> Self {
        let limit = config().config.general.auth_rate_limit;
        let quota = Quota::per_minute(NonZeroU32::new(limit).unwrap_or(nonzero!(10u32)));

        Self {
            limiters: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(MAX_TRACKED_IPS)
                    .expect("MAX_TRACKED_IPS must be non-zero"),
            ))),
            quota,
        }
    }

    /// Check if an IP address can attempt authentication.
    /// Returns true if the request is allowed, false if rate limited.
    pub fn check(&self, ip: IpAddr) -> bool {
        let mut limiters = self.limiters.lock();

        let limiter = limiters
            .get_or_insert(ip, || Arc::new(RateLimiter::direct(self.quota)));

        limiter.check().is_ok()
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_initial_requests() {
        let limiter = AuthRateLimiter::new();
        let ip = "127.0.0.1".parse().unwrap();

        // First 10 requests should succeed
        for _ in 0..10 {
            assert!(limiter.check(ip));
        }

        // 11th request should be rate limited
        assert!(!limiter.check(ip));
    }

    #[test]
    fn test_rate_limiter_per_ip() {
        let limiter = AuthRateLimiter::new();
        let ip1: IpAddr = "127.0.0.1".parse().unwrap();
        let ip2: IpAddr = "127.0.0.2".parse().unwrap();

        // Exhaust ip1
        for _ in 0..10 {
            assert!(limiter.check(ip1));
        }
        assert!(!limiter.check(ip1));

        // ip2 should still work
        for _ in 0..10 {
            assert!(limiter.check(ip2));
        }
        assert!(!limiter.check(ip2));
    }

    // Commented out: this test would take 1 minute to complete
    // #[tokio::test]
    // async fn test_rate_limiter_recovers() {
    //     let limiter = AuthRateLimiter::new();
    //     let ip = "127.0.0.1".parse().unwrap();
    //
    //     // Exhaust limit
    //     for _ in 0..10 {
    //         assert!(limiter.check(ip));
    //     }
    //     assert!(!limiter.check(ip));
    //
    //     // Wait for quota to replenish
    //     tokio::time::sleep(Duration::from_secs(60)).await;
    //     assert!(limiter.check(ip));
    // }
}