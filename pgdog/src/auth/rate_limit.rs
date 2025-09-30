//! Rate limiting for authentication attempts.

use governor::{
    clock::DefaultClock,
    state::keyed::DefaultKeyedStateStore,
    Quota, RateLimiter,
};
use once_cell::sync::Lazy;
use std::net::{IpAddr, Ipv6Addr};
use std::num::NonZeroU32;

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

/// Global rate limiter for authentication attempts per IP address.
pub static AUTH_RATE_LIMITER: Lazy<RateLimiter<IpAddr, KeyedStore, DefaultClock>> = Lazy::new(|| {
    let limit = config().config.general.auth_rate_limit;
    // Config validation ensures limit is always >= 1
    let limit = NonZeroU32::new(limit).expect("auth_rate_limit validated to be non-zero");
    let quota = Quota::per_minute(limit).allow_burst(limit);
    RateLimiter::keyed(quota)
});

/// Check if an IP address can attempt authentication.
/// Returns true if the request is allowed, false if rate limited.
///
/// IPv6 addresses are normalized to /64 prefix before rate limiting
/// to prevent attackers from bypassing limits by rotating through
/// addresses in their allocated block.
pub fn check(ip: IpAddr) -> bool {
    let normalized_ip = normalize_ip(ip);
    AUTH_RATE_LIMITER.check_key(&normalized_ip).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_initial_requests() {
        let ip = "127.0.0.1".parse().unwrap();

        // First 10 requests should succeed
        for _ in 0..10 {
            assert!(check(ip));
        }

        // 11th request should be rate limited
        assert!(!check(ip));
    }

    #[test]
    fn test_rate_limiter_per_ip() {
        let ip1: IpAddr = "127.0.0.10".parse().unwrap();
        let ip2: IpAddr = "127.0.0.20".parse().unwrap();

        // Exhaust ip1
        for _ in 0..10 {
            assert!(check(ip1));
        }
        assert!(!check(ip1));

        // ip2 should still work
        for _ in 0..10 {
            assert!(check(ip2));
        }
        assert!(!check(ip2));
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
        // Two addresses in same /64 block
        let ip1: IpAddr = "2001:db8:cafe:1234:5678:90ab:cdef:1111".parse().unwrap();
        let ip2: IpAddr = "2001:db8:cafe:1234:9999:aaaa:bbbb:cccc".parse().unwrap();

        // Exhaust rate limit with first address
        for _ in 0..10 {
            assert!(check(ip1));
        }
        assert!(!check(ip1));

        // Second address in same /64 should also be blocked
        assert!(!check(ip2));

        // Different /64 should still work
        let ip3: IpAddr = "2001:db8:cafe:5678:1234:5678:90ab:cdef".parse().unwrap();
        assert!(check(ip3));
    }

    // Note: Time-based recovery test is not included because:
    // 1. Governor crate uses DefaultClock (wall-clock time), not Tokio's time
    // 2. Tokio's pause()/advance() don't affect governor's clock
    // 3. Would need custom clock implementation or 60s wait
    //
    // The rate limiting behavior is well-tested by the governor crate itself.
    // Our tests verify correct integration with the library.
}