//! DNS cache for backend server hostnames.
//!
//! PgDog uses this cache when `general.dns_ttl` is configured. Cached hostname
//! entries are reused until the configured TTL expires; a TTL of zero disables
//! hostname cache hits so callers resolve DNS on every request. IP literals are
//! returned directly and are not stored in the cache.

use hickory_resolver::{Resolver, name_server::TokioConnectionProvider};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::IpAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use crate::backend::Error;
use crate::config::config;

static DNS_CACHE: Lazy<Arc<DnsCache>> = Lazy::new(|| Arc::new(DnsCache::new()));

/// Cached DNS lookup result with the time it was stored.
#[derive(Debug, Clone, Copy)]
pub struct CacheEntry {
    time: Instant,
    ip: IpAddr,
}

impl Deref for CacheEntry {
    type Target = IpAddr;

    fn deref(&self) -> &Self::Target {
        &self.ip
    }
}

/// Shared hostname-to-IP cache backed by the system DNS resolver.
pub struct DnsCache {
    resolver: Arc<Resolver<TokioConnectionProvider>>,
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
}

impl Default for DnsCache {
    fn default() -> Self {
        Self::new()
    }
}

impl DnsCache {
    /// Retrieve the global DNS cache instance.
    pub fn global() -> Arc<DnsCache> {
        DNS_CACHE.clone()
    }

    /// Create a new DNS cache instance.
    pub fn new() -> Self {
        // Initialize the Resolver with system config (e.g., /etc/resolv.conf on Unix)
        let resolver = Resolver::builder(TokioConnectionProvider::default())
            .unwrap()
            .build();

        DnsCache {
            resolver: Arc::new(resolver),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Resolve hostname to socket address string.
    pub async fn resolve(&self, hostname: &str) -> Result<IpAddr, Error> {
        if let Some(ip) = self.get_cached_ip(hostname) {
            return Ok(ip);
        }

        let ip = self.resolve_and_cache(hostname).await?;
        Ok(ip)
    }
}

impl DnsCache {
    /// Get the DNS refresh interval.
    fn ttl() -> Duration {
        config()
            .config
            .general
            .dns_ttl()
            .unwrap_or(Duration::from_secs(60)) // INVARIANT: We don't call [`DnsCache::resolve`] unless the DNS ttl is set.
    }

    /// Get IP address from cache only.
    ///
    /// Returns `None` when the hostname is not cached, the entry has expired, or the TTL is
    /// configured as zero. A zero TTL means hostname cache entries are never reused and callers
    /// resolve DNS on every request. IP literals are returned directly regardless of TTL because
    /// they do not require DNS resolution.
    fn get_cached_ip(&self, hostname: &str) -> Option<IpAddr> {
        self.get_cached_ip_with_ttl(hostname, Self::ttl())
    }

    fn get_cached_ip_with_ttl(&self, hostname: &str, ttl: Duration) -> Option<IpAddr> {
        if let Ok(ip) = hostname.parse::<IpAddr>() {
            return Some(ip);
        }

        if let Some(entry) = self.cache.read().get(hostname) {
            if ttl.is_zero() || entry.time.elapsed() > ttl {
                return None;
            } else {
                return Some(entry.ip);
            }
        }

        None
    }

    /// Do the actual DNS resolution and cache the result.
    async fn resolve_and_cache(&self, hostname: &str) -> Result<IpAddr, Error> {
        let response = self.resolver.lookup_ip(hostname).await?;

        let ip = response
            .iter()
            .next()
            .ok_or(Error::DnsResolutionFailed(hostname.to_string()))?;

        self.cache_ip(hostname, ip);

        Ok(ip)
    }

    fn cache_ip(&self, hostname: &str, ip: IpAddr) {
        let mut cache = self.cache.write();
        cache.insert(
            hostname.to_string(),
            CacheEntry {
                ip,
                time: Instant::now(),
            },
        );
    }
}

#[cfg(test)]
impl DnsCache {
    pub fn clear_cache_for_testing(&self) {
        let mut cache = self.cache.write();
        cache.clear();
    }

    pub fn cached_ip_for_testing(&self, hostname: &str) -> Option<IpAddr> {
        self.get_cached_ip(hostname)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use std::net::{IpAddr, Ipv4Addr};
    use tokio::time::Instant;
    use tokio::time::timeout;

    #[tokio::test]
    async fn resolve_returns_ip_address_directly() {
        let cache = DnsCache::new();

        let ip = cache.resolve("127.0.0.1").await.expect("IPv4 resolves");
        assert_eq!(ip, IpAddr::V4(Ipv4Addr::LOCALHOST));

        let ipv6 = cache.resolve("::1").await.expect("IPv6 resolves");
        assert_eq!(ipv6, IpAddr::V6(std::net::Ipv6Addr::LOCALHOST));

        assert!(cache.cache.read().is_empty());
    }

    #[tokio::test]
    async fn get_cached_ip_returns_fresh_entry() {
        let cache = DnsCache::new();
        let hostname = "cached.example";
        let ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));

        cache.cache_ip(hostname, ip);

        assert_eq!(cache.get_cached_ip(hostname), Some(ip));
    }

    #[tokio::test]
    async fn get_cached_ip_ignores_expired_entry() {
        let cache = DnsCache::new();
        let hostname = "expired.example";
        let ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));

        cache.cache.write().insert(
            hostname.to_string(),
            CacheEntry {
                ip,
                time: Instant::now() - Duration::from_secs(3600),
            },
        );

        assert_eq!(cache.get_cached_ip(hostname), None);
    }

    #[tokio::test]
    async fn resolve_uses_cached_ip_before_entry_expires() {
        let cache = DnsCache::new();
        let hostname = "cached.invalid";
        let ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));

        cache.cache_ip(hostname, ip);

        let resolved = cache.resolve(hostname).await.expect("fresh cache entry");

        assert_eq!(resolved, ip);
    }

    #[tokio::test]
    async fn resolve_caches_successful_dns_lookup() {
        let cache = DnsCache::new();
        let hostname = "localhost";

        assert!(cache.cache.read().get(hostname).is_none());

        let resolved = timeout(Duration::from_secs(5), cache.resolve(hostname))
            .await
            .expect("DNS lookup completes")
            .expect("localhost resolves");

        let cached = cache.cache.read().get(hostname).copied();

        assert_eq!(cached.map(|entry| entry.ip), Some(resolved));
    }

    #[tokio::test]
    async fn resolve_attempts_dns_after_entry_expires() {
        let cache = DnsCache::new();
        let hostname = "this-domain-definitely-does-not-exist-12345.invalid";
        let cached_ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));

        cache.cache.write().insert(
            hostname.to_string(),
            CacheEntry {
                ip: cached_ip,
                time: Instant::now() - Duration::from_secs(3600),
            },
        );

        let result = timeout(Duration::from_secs(5), cache.resolve(hostname))
            .await
            .expect("DNS lookup completes");

        assert!(
            result.is_err(),
            "expired cache entry should force DNS resolution"
        );
    }

    #[test]
    fn get_cached_ip_ignores_entry_when_dns_ttl_is_zero() {
        let cache = DnsCache::new();
        let hostname = "zero-ttl.example";
        let ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));

        cache.cache_ip(hostname, ip);

        assert_eq!(cache.get_cached_ip_with_ttl(hostname, Duration::ZERO), None);
    }
}
