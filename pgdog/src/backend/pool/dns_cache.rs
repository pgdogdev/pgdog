use futures::future::join_all;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::lookup_host;
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
        DnsCache {
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
        let interval = config()
            .config
            .general
            .dns_ttl()
            .unwrap_or(Duration::from_secs(60));

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
        let addr = format!("{}:0", hostname);
        let resolved = lookup_host(&addr)
            .await?
            .next()
            .ok_or_else(|| Error::DnsLookupError(hostname.to_string()))?;

        let ip = resolved.ip();
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
}

// -------------------------------------------------------------------------------------------------
// ------ DnsCache :: Test-only methods ------------------------------------------------------------

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
