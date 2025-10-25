//! Rate limiting for authentication attempts.

use governor::{
    clock::DefaultClock,
    state::keyed::DefaultKeyedStateStore,
    Quota, RateLimiter,
};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::net::{IpAddr, Ipv6Addr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::config;

/// Normalize an IP address for rate limiting purposes.
///
/// IPv4 addresses are used as-is.
/// IPv6 addresses are masked to their /64 prefix to prevent attackers
/// from bypassing rate limits by rotating through addresses in their
/// allocated block (most ISPs and cloud providers allocate /64 or /48).
fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(v4) => IpAddr::V4(v4),
        IpAddr::V6(v6) => {
            let segments = v6.segments();
            // Keep first 4 segments (64 bits), zero out the rest
            let masked = Ipv6Addr::new(segments[0], segments[1], segments[2], segments[3], 0, 0, 0, 0);
            IpAddr::V6(masked)
        }
    }
}

type KeyedStore = DefaultKeyedStateStore<IpAddr>;
type IpRateLimiter = RateLimiter<IpAddr, KeyedStore, DefaultClock>;

fn create_limiter(limit: u32) -> IpRateLimiter {
    let limit = NonZeroU32::new(limit).expect("limit must be non-zero");
    let quota = Quota::per_minute(limit).allow_burst(limit);
    RateLimiter::keyed(quota)
}

/// Global rate limiter for authentication attempts per IP address.
static AUTH_RATE_LIMITER: Lazy<Arc<RwLock<Option<IpRateLimiter>>>> = Lazy::new(|| {
    let limit = config().config.general.auth_rate_limit;
    Arc::new(RwLock::new(limit.map(create_limiter)))
});

/// Track last reset time to prevent unbounded memory growth.
///
/// Governor's DefaultKeyedStateStore (DashMap) grows indefinitely.
/// Reset limiter every hour to clear accumulated state.
static LAST_RESET: Lazy<Arc<RwLock<Instant>>> = Lazy::new(|| Arc::new(RwLock::new(Instant::now())));

/// Check if an IP address can attempt authentication.
/// Returns true if the request is allowed, false if rate limited.
///
/// IPv6 addresses are normalized to /64 prefix before rate limiting
/// to prevent attackers from bypassing limits by rotating through
/// addresses in their allocated block.
///
/// Resets state every hour to prevent unbounded memory growth.
pub fn check(ip: IpAddr) -> bool {
    // Reset every hour to prevent unbounded growth of keyed state
    if LAST_RESET.read().elapsed() > Duration::from_secs(3600) {
        reload(config().config.general.auth_rate_limit);
        *LAST_RESET.write() = Instant::now();
    }

    let guard = AUTH_RATE_LIMITER.read();
    if let Some(ref limiter) = *guard {
        check_with_limiter(limiter, ip)
    } else {
        true
    }
}

/// Internal function to check rate limit with a specific limiter.
/// Allows testing without depending on global config.
fn check_with_limiter(limiter: &IpRateLimiter, ip: IpAddr) -> bool {
    let normalized_ip = normalize_ip(ip);
    limiter.check_key(&normalized_ip).is_ok()
}

/// Reload the rate limiter with a new limit.
///
/// Called when configuration is reloaded via SIGHUP or RELOAD command.
pub fn reload(new_limit: Option<u32>) {
    *AUTH_RATE_LIMITER.write() = new_limit.map(create_limiter);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a test rate limiter with a specific limit.
    /// Tests should use this to avoid coupling to config defaults.
    fn test_limiter(limit: u32) -> IpRateLimiter {
        create_limiter(limit)
    }

    #[test]
    fn test_rate_limiter_allows_initial_requests() {
        let limiter = test_limiter(10);
        let ip = "127.0.0.1".parse().unwrap();

        // First 10 requests should succeed
        for _ in 0..10 {
            assert!(check_with_limiter(&limiter, ip));
        }

        // 11th request should be rate limited
        assert!(!check_with_limiter(&limiter, ip));
    }

    #[test]
    fn test_rate_limiter_per_ip() {
        let limiter = test_limiter(10);
        let ip1: IpAddr = "127.0.0.10".parse().unwrap();
        let ip2: IpAddr = "127.0.0.20".parse().unwrap();

        // Exhaust ip1
        for _ in 0..10 {
            assert!(check_with_limiter(&limiter, ip1));
        }
        assert!(!check_with_limiter(&limiter, ip1));

        // ip2 should still work
        for _ in 0..10 {
            assert!(check_with_limiter(&limiter, ip2));
        }
        assert!(!check_with_limiter(&limiter, ip2));
    }

    #[test]
    fn test_ipv6_normalization() {
        // Test that IPv6 addresses in same /64 are normalized to same value
        let ip1: IpAddr = "2001:db8:abcd:1234:5678:90ab:cdef:1111".parse().unwrap();
        let ip2: IpAddr = "2001:db8:abcd:1234:9999:aaaa:bbbb:cccc".parse().unwrap();
        let ip3: IpAddr = "2001:db8:abcd:5678:1234:5678:90ab:cdef".parse().unwrap();

        let normalized1 = normalize_ip(ip1);
        let normalized2 = normalize_ip(ip2);
        let normalized3 = normalize_ip(ip3);

        // Same /64 prefix should normalize to same address
        assert_eq!(normalized1, normalized2);
        assert_eq!(normalized1.to_string(), "2001:db8:abcd:1234::");

        // Different /64 prefix should normalize differently
        assert_ne!(normalized1, normalized3);
        assert_eq!(normalized3.to_string(), "2001:db8:abcd:5678::");
    }

    #[test]
    fn test_ipv4_normalization() {
        // IPv4 addresses should pass through unchanged
        let ip: IpAddr = "192.168.1.100".parse().unwrap();
        let normalized = normalize_ip(ip);
        assert_eq!(ip, normalized);
    }

    #[test]
    fn test_ipv6_rate_limiting_blocks_same_subnet() {
        let limiter = test_limiter(10);

        // Two addresses in same /64 block
        let ip1: IpAddr = "2001:db8:cafe:1234:5678:90ab:cdef:1111".parse().unwrap();
        let ip2: IpAddr = "2001:db8:cafe:1234:9999:aaaa:bbbb:cccc".parse().unwrap();

        // Exhaust rate limit with first address
        for _ in 0..10 {
            assert!(check_with_limiter(&limiter, ip1));
        }
        assert!(!check_with_limiter(&limiter, ip1));

        // Second address in same /64 should also be blocked
        assert!(!check_with_limiter(&limiter, ip2));

        // Different /64 should still work
        let ip3: IpAddr = "2001:db8:cafe:5678:1234:5678:90ab:cdef".parse().unwrap();
        assert!(check_with_limiter(&limiter, ip3));
    }

    #[test]
    fn test_reload() {
        // Use unique IP to avoid interference from other tests
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        // Exhaust default limit of 10 on global limiter
        for _ in 0..10 {
            assert!(check(ip));
        }
        assert!(!check(ip));

        // Reload with higher limit (resets state)
        reload(Some(20));

        // Should now allow more requests (state was reset)
        for _ in 0..20 {
            assert!(check(ip));
        }
        assert!(!check(ip));
    }

    #[test]
    fn test_different_limits() {
        // Test with limit of 3
        let limiter = test_limiter(3);
        let ip: IpAddr = "192.168.100.1".parse().unwrap();

        for _ in 0..3 {
            assert!(check_with_limiter(&limiter, ip));
        }
        assert!(!check_with_limiter(&limiter, ip));
    }

    #[test]
    fn test_limit_of_one() {
        // Edge case: minimum limit of 1
        let limiter = test_limiter(1);
    #[test]
    fn test_unlimited_mode_allows_all() {
        // Temporarily set global limiter to None and ensure check() always returns true
        reload(None);
        let ip: IpAddr = "203.0.113.1".parse().unwrap();
        for _ in 0..1000 {
            assert!(check(ip));
        }
        // Restore to default reasonable limit for other tests
        reload(Some(10));
    }
        let ip: IpAddr = "192.168.100.2".parse().unwrap();

        assert!(check_with_limiter(&limiter, ip));
        assert!(!check_with_limiter(&limiter, ip));
    }

    // Note: Time-based recovery test is not included because:
    // 1. Governor crate uses DefaultClock (wall-clock time), not Tokio's time
    // 2. Tokio's pause()/advance() don't affect governor's clock
    // 3. Would need custom clock implementation or 60s wait
    //
    // The rate limiting behavior is well-tested by the governor crate itself.
    // Our tests verify correct integration with the library.
}