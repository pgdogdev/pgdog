use futures::future::join_all;
use hickory_resolver::{name_server::TokioConnectionProvider, ResolveError, Resolver};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error};

use crate::backend::Error;
use crate::config::config;

// -------------------------------------------------------------------------------------------------
// ------ Singleton(s) -----------------------------------------------------------------------------

static DNS_CACHE: Lazy<Arc<DnsCache>> = Lazy::new(|| Arc::new(DnsCache::new()));

// -------------------------------------------------------------------------------------------------
// ------ DnsCache :: Public interface -------------------------------------------------------------

pub struct DnsCache {
    resolver: Arc<Resolver<TokioConnectionProvider>>,
    cache: Arc<RwLock<HashMap<String, IpAddr>>>,
    hostnames: Arc<RwLock<HashSet<String>>>,
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
            hostnames: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Resolve hostname to socket address string.
    pub async fn resolve(&self, hostname: &str) -> Result<IpAddr, Error> {
        if let Some(ip) = self.get_cached_ip(hostname) {
            return Ok(ip);
        }

        // Track hostname for future refreshes.
        {
            let mut hostnames = self.hostnames.write();
            if hostnames.insert(hostname.to_string()) {
                debug!("dns cache added hostname \"{}\"", hostname);
            }
        }

        let ip = self.resolve_and_cache(hostname).await?;
        Ok(ip)
    }

    /// Start the background refresh loop.
    /// This spawns an infinite loop that runs until the process exits.
    /// For testing, use short TTLs and allow the test runtime to handle shutdown.
    pub fn start_refresh_loop(self: &Arc<Self>) {
        let interval = Self::ttl();

        let cache_ref = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                println!("-> I AM REFRESHING!!! {:?}", interval);
                sleep(interval).await;
                cache_ref.refresh_all_hostnames().await;
            }
        });
    }
}

// -------------------------------------------------------------------------------------------------
// ------ DnsCache :: Private methods --------------------------------------------------------------

impl DnsCache {
    /// Get the DNS refresh interval (short in tests, config-based otherwise).
    fn ttl() -> Duration {
        if cfg!(test) {
            return Duration::from_millis(50);
        }

        config()
            .config
            .general
            .dns_ttl()
            .unwrap_or(Duration::from_secs(60)) // Should never happen in practice
    }

    /// Get IP address from cache only. Returns None if not cached or expired.
    fn get_cached_ip(&self, hostname: &str) -> Option<IpAddr> {
        if let Ok(ip) = hostname.parse::<IpAddr>() {
            return Some(ip);
        }

        self.cache.read().get(hostname).copied()
    }

    /// Refresh all hostnames in the refresh list.
    async fn refresh_all_hostnames(self: &Arc<Self>) {
        let hostnames_to_refresh: Vec<String> = self.hostnames.read().iter().cloned().collect();

        let tasks: Vec<_> = hostnames_to_refresh
            .into_iter()
            .map(|hostname| {
                let cache_ref = Arc::clone(self);
                tokio::spawn(async move {
                    if let Err(e) = cache_ref.resolve_and_cache(&hostname).await {
                        error!("failed to refresh DNS for {}: {}", hostname, e);
                        println!("failed to refresh DNS for {}: {}", hostname, e);
                    } else {
                        println!("refreshed hostname: {}", hostname);
                        debug!("refreshed hostname: {}", hostname);
                    }
                })
            })
            .collect();

        join_all(tasks).await;
    }

    /// Do the actual DNS resolution and cache the result.
    async fn resolve_and_cache(&self, hostname: &str) -> Result<IpAddr, Error> {
        let response = self
            .resolver
            .lookup_ip(hostname)
            .await
            .map_err(|e: ResolveError| Error::DnsLookupError(format!("{}: {}", hostname, e)))?;

        let ip = response
            .iter()
            .next()
            .ok_or_else(|| Error::DnsLookupError(format!("{}: no IPs found", hostname)))?;

        self.cache_ip(hostname, ip);

        Ok(ip)
    }

    fn cache_ip(&self, hostname: &str, ip: IpAddr) {
        let mut cache = self.cache.write();
        cache.insert(hostname.to_string(), ip);
    }
}

// -------------------------------------------------------------------------------------------------
// ------ DnsCache :: Tests ------------------------------------------------------------------------

#[cfg(test)]
mod tests {

    use super::*;

    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_resolve_ip_address_directly() {
        let cache = DnsCache::new();

        // Test that IP addresses are returned as-is without DNS lookup
        let ip = cache.resolve("127.0.0.1").await.unwrap();
        assert_eq!(ip.to_string(), "127.0.0.1");

        let ipv6 = cache.resolve("::1").await.unwrap();
        assert_eq!(ipv6.to_string(), "::1");
    }

    #[tokio::test]
    async fn test_resolve_localhost() {
        let cache = DnsCache::new();

        // localhost should always resolve
        let result = cache.resolve("localhost").await;
        assert!(result.is_ok());

        let ip = result.unwrap();
        // localhost can resolve to either 127.0.0.1 or ::1
        assert!(ip.to_string() == "127.0.0.1" || ip.to_string() == "::1");
    }

    #[tokio::test]
    async fn test_resolve_well_known_domains() {
        let cache = DnsCache::new();

        // Test with well-known domains that should always resolve
        let domains = ["google.com", "cloudflare.com", "github.com"];

        for domain in domains {
            let result = timeout(Duration::from_secs(5), cache.resolve(domain)).await;
            assert!(result.is_ok(), "Timeout resolving {}", domain);

            let ip_result = result.unwrap();
            assert!(ip_result.is_ok(), "Failed to resolve {}", domain);
        }
    }

    #[tokio::test]
    async fn test_resolve_invalid_hostname() {
        let cache = DnsCache::new();

        let result = cache
            .resolve("this-domain-definitely-does-not-exist-12345.invalid")
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_caching_behavior() {
        let cache = DnsCache::new();

        let hostname = "google.com";

        // First resolution
        let start = std::time::Instant::now();
        let ip1 = cache.resolve(hostname).await.unwrap();
        let first_duration = start.elapsed();

        // Second resolution should be cached and faster
        let start = std::time::Instant::now();
        let ip2 = cache.resolve(hostname).await.unwrap();
        let second_duration = start.elapsed();

        // Should be the same IP
        assert_eq!(ip1, ip2);

        // Second call should be significantly faster (cached)
        assert!(
            second_duration < first_duration / 2,
            "Second call should be much faster due to caching. First: {:?}, Second: {:?}",
            first_duration,
            second_duration
        );
    }

    #[tokio::test]
    async fn test_concurrent_resolutions() {
        let cache = Arc::new(DnsCache::new());

        // Test multiple concurrent resolutions
        let hostnames = vec!["google.com", "github.com", "cloudflare.com"];

        let tasks: Vec<_> = hostnames
            .into_iter()
            .map(|hostname| {
                let cache_clone = cache.clone();
                tokio::spawn(async move { cache_clone.resolve(hostname).await })
            })
            .collect();

        let results = join_all(tasks).await;

        // All should succeed
        for result in results {
            assert!(result.is_ok());
            assert!(result.unwrap().is_ok());
        }
    }

    #[tokio::test]
    async fn test_refresh_updates_cache() {
        let cache = Arc::new(DnsCache::new());

        cache.start_refresh_loop();

        // Initial resolve and cache
        let hostname = "github.com";
        let ip1 = cache.resolve(hostname).await.unwrap();

        // Clear for simulation (in reality, DNS might change)
        cache.clear_cache_for_testing();

        // Manually trigger refresh
        cache.refresh_all_hostnames().await;

        let ip2 = cache.resolve(hostname).await.unwrap();
        assert_eq!(
            ip1, ip2,
            "IP should be re-resolved and cached after refresh"
        );

        // Check hostname was tracked
        let cached_hostnames = cache.get_cached_hostnames_for_testing();
        assert!(cached_hostnames.contains(&hostname.to_string()));
    }

    #[tokio::test]
    async fn test_hostname_uniqueness() {
        let cache = DnsCache::new();
        let hostname = "example.com";

        // Resolve twice
        cache.resolve(hostname).await.unwrap();
        cache.resolve(hostname).await.unwrap();

        let cached_hostnames = cache.get_cached_hostnames_for_testing();
        assert_eq!(
            cached_hostnames.len(),
            1,
            "Hostname should be added only once"
        );
        assert!(cached_hostnames.contains(&hostname.to_string()));
    }

    #[tokio::test]
    async fn test_ipv6_only_resolution() {
        let cache = DnsCache::new();
        let hostname = "ipv6.google.com"; // Known IPv6-only or dual-stack

        let ip = cache.resolve(hostname).await.unwrap();
        assert!(ip.is_ipv6(), "Expected IPv6 address, got {}", ip);
    }

    #[tokio::test]
    async fn test_refresh_handles_errors() {
        let cache = Arc::new(DnsCache::new());

        // Add an invalid hostname to trigger error
        {
            let mut hostnames = cache.hostnames.write();
            hostnames.insert("invalid-host-xyz.invalid".to_string());
        }

        // Trigger refresh; it should not panic
        cache.refresh_all_hostnames().await;

        // Verify cache isn't polluted (no entry added on error)
        assert!(cache.get_cached_ip("invalid-host-xyz.invalid").is_none());
    }
}

// -------------------------------------------------------------------------------------------------
// ------ DnsCache :: Test-specific methods --------------------------------------------------------

impl DnsCache {
    #[cfg(test)]
    pub fn clear_cache_for_testing(&self) {
        let mut cache = self.cache.write();
        cache.clear();
    }

    #[cfg(test)]
    pub fn get_cached_hostnames_for_testing(&self) -> Vec<String> {
        let hostnames = self.hostnames.read();
        hostnames.iter().cloned().collect()
    }
}

// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------
