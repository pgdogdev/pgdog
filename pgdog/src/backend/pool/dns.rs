use futures::future::join_all;
use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::net::lookup_host;
use tokio::select;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

// -------------------------------------------------------------------------------------------------
// ------ Constant(s) ------------------------------------------------------------------------------

const DEFAULT_TTL: Duration = Duration::from_secs(30);

// -------------------------------------------------------------------------------------------------
// ------ Singleton(s) -----------------------------------------------------------------------------

static DNS_CACHE: Lazy<Arc<DnsCache>> = Lazy::new(|| {
    Arc::new(DnsCache {
        cache: Arc::new(RwLock::new(HashMap::new())),
        hostnames: Arc::new(RwLock::new(HashSet::new())),
        ttl: Arc::new(RwLock::new(DEFAULT_TTL)),
    })
});

// -------------------------------------------------------------------------------------------------
// ------ DnsCache :: Public interface -------------------------------------------------------------

#[derive(Clone, Debug)]
struct DnsCacheEntry {
    ip: IpAddr,
    resolved_at: Instant,
    ttl: Duration,
}

pub struct DnsCache {
    cache: Arc<RwLock<HashMap<String, DnsCacheEntry>>>,
    hostnames: Arc<RwLock<HashSet<String>>>,
    ttl: Arc<RwLock<Duration>>,
}

impl DnsCache {
    /// Retrieve the global DNS cache instance.
    pub fn global() -> Arc<DnsCache> {
        DNS_CACHE.clone()
    }

    /// Resolve hostname to socket address string.
    pub async fn resolve(&self, hostname: &str) -> Result<IpAddr, std::io::Error> {
        if let Some(ip) = self.get_cached_ip(hostname) {
            return Ok(ip);
        }

        // Track hostname for future refreshes.
        {
            let mut hostnames = self.hostnames.write().unwrap();
            if hostnames.insert(hostname.to_string()) {
                info!("DNS Cache :: Added hostname: {}", hostname);
            }
        }

        let ip = self.resolve_and_cache(hostname).await?;
        Ok(ip)
    }

    /// Update the TTL for new cache entries.
    pub fn set_ttl(&self, ttl: Duration) {
        let mut current_ttl = self.ttl.write().unwrap();
        *current_ttl = ttl;
    }

    /// Start the background refresh loop.
    /// Returns a CancellationToken that can be used to stop the loop (needed for testing).
    /// In production, you can ignore the return value with `_`.
    pub fn start_refresh_loop(self: &Arc<Self>) -> tokio_util::sync::CancellationToken {
        let cancel_token = CancellationToken::new();
        let cancel_clone = cancel_token.clone();

        let cache_ref = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                let interval = cache_ref.ttl();

                select! {
                    _ = sleep(interval) => {
                        cache_ref.refresh_all_hostnames().await;
                    }
                    _ = cancel_clone.cancelled() => {
                        warn!("DNS Cache :: Refresh loop cancelled!");
                        break;
                    }
                }
            }
        });

        cancel_token
    }
}

// -------------------------------------------------------------------------------------------------
// ------ DnsCache :: Private methods --------------------------------------------------------------

impl DnsCache {
    /// Get IP address from cache only. Returns None if not cached or expired.
    fn get_cached_ip(&self, hostname: &str) -> Option<IpAddr> {
        // Already an IP? Just parse and return
        if let Ok(ip) = hostname.parse::<IpAddr>() {
            return Some(ip);
        }

        let cache = self.cache.read().unwrap();
        if let Some(entry) = cache.get(hostname) {
            if entry.resolved_at.elapsed() < entry.ttl {
                return Some(entry.ip);
            }
        }

        None
    }

    /// Refresh all hostnames in the refresh list.
    async fn refresh_all_hostnames(self: &Arc<Self>) {
        let tasks: Vec<_> = {
            let hostnames_guard = self.hostnames.read().unwrap();
            hostnames_guard
                .iter()
                .map(|hostname| {
                    let hostname = hostname.clone();
                    let cache_ref = Arc::clone(self);
                    tokio::spawn(async move {
                        // Skip if already cached and not expired
                        if cache_ref.get_cached_ip(&hostname).is_some() {
                            debug!("Skipped refresh for fresh entry: {}", hostname);
                            return;
                        }

                        if let Err(e) = cache_ref.resolve_and_cache(&hostname).await {
                            error!("Failed to refresh DNS for {}: {}", hostname, e);
                        } else {
                            debug!("Refreshed hostname: {}", hostname);
                        }
                    })
                })
                .collect()
        };

        join_all(tasks).await;
    }

    /// Do the actual DNS resolution and cache the result.
    async fn resolve_and_cache(&self, hostname: &str) -> Result<IpAddr, std::io::Error> {
        let addr = format!("{}:0", hostname);
        let resolved = lookup_host(&addr)
            .await?
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "No addresses found"))?;

        let ip = resolved.ip();
        self.cache_ip(hostname, ip);

        Ok(ip)
    }

    fn cache_ip(&self, hostname: &str, ip: IpAddr) {
        let ttl = self.ttl();
        let mut cache = self.cache.write().unwrap();
        cache.insert(
            hostname.to_string(),
            DnsCacheEntry {
                ip,
                resolved_at: Instant::now(),
                ttl,
            },
        );
    }

    /// Get the current TTL.
    fn ttl(&self) -> Duration {
        *self.ttl.read().unwrap()
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
        let cache = DnsCache::new_for_testing(Duration::from_secs(60));

        // Test that IP addresses are returned as-is without DNS lookup
        let ip = cache.resolve("127.0.0.1").await.unwrap();
        assert_eq!(ip.to_string(), "127.0.0.1");

        let ipv6 = cache.resolve("::1").await.unwrap();
        assert_eq!(ipv6.to_string(), "::1");
    }

    #[tokio::test]
    async fn test_resolve_localhost() {
        let cache = DnsCache::new_for_testing(Duration::from_secs(60));

        // localhost should always resolve
        let result = cache.resolve("localhost").await;
        assert!(result.is_ok());

        let ip = result.unwrap();
        // localhost can resolve to either 127.0.0.1 or ::1
        assert!(ip.to_string() == "127.0.0.1" || ip.to_string() == "::1");
    }

    #[tokio::test]
    async fn test_resolve_well_known_domains() {
        let cache = DnsCache::new_for_testing(Duration::from_secs(60));

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
        let cache = DnsCache::new_for_testing(Duration::from_secs(60));

        let result = cache
            .resolve("this-domain-definitely-does-not-exist-12345.invalid")
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_caching_behavior() {
        let cache = DnsCache::new_for_testing(Duration::from_secs(60));

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
        let cache = DnsCache::new_for_testing(Duration::from_secs(60));

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
    async fn test_refresh_loop_updates_cache() {
        // Create cache with short TTL for faster testing
        let ttl = Duration::from_millis(50);
        let cache = DnsCache::new_for_testing(ttl);

        // Start the refresh loop
        let cancel_token = cache.start_refresh_loop();

        // Resolve a hostname to populate the cache
        let hostname = "localhost";
        let initial_result = cache.resolve(hostname).await;
        assert!(initial_result.is_ok());

        // Verify hostname is tracked for refresh
        let tracked_hostnames = cache.get_cached_hostnames_for_testing();
        assert!(tracked_hostnames.contains(&hostname.to_string()));

        // Clear the cache manually to simulate an expired entry
        cache.clear_cache_for_testing();

        // Assert that the cache is now empty
        assert!(
            cache.is_cache_empty_for_testing(),
            "Cache should be empty after clearing"
        );

        // Without the refresh loop, this would fail since cache is empty
        // But the refresh loop should repopulate it within ~100ms
        let refresh_timeout = Duration::from_millis(1000);
        let result = timeout(refresh_timeout, async {
            loop {
                if let Some(_) = cache.get_cached_ip(hostname) {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(
            result.is_ok(),
            "Refresh loop should have repopulated the cache"
        );

        // Clean shutdown
        cancel_token.cancel();

        // Give the loop time to exit cleanly
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

impl DnsCache {
    #[cfg(test)]
    pub fn clear_cache_for_testing(&self) {
        let mut cache = self.cache.write().unwrap();
        cache.clear();
    }

    #[cfg(test)]
    pub fn get_cached_hostnames_for_testing(&self) -> Vec<String> {
        let hostnames = self.hostnames.read().unwrap();
        hostnames.iter().cloned().collect()
    }

    /// Create a test cache with custom TTL
    #[cfg(test)]
    pub fn new_for_testing(ttl: Duration) -> Arc<DnsCache> {
        Arc::new(DnsCache {
            cache: Arc::new(RwLock::new(HashMap::new())),
            hostnames: Arc::new(RwLock::new(HashSet::new())),
            ttl: Arc::new(RwLock::new(ttl)),
        })
    }

    #[cfg(test)]
    pub fn is_cache_empty_for_testing(&self) -> bool {
        let cache = self.cache.read().unwrap();
        cache.is_empty()
    }
}

// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------
