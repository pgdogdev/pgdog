use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, SystemTime};

use crate::backend::{pool::Address, Error};

/// How early to wake up before a token expires to fetch a fresh one.
/// Applied by [`TokenCache::refresh_at`] so callers never need to
/// know or re-apply this value.
const EXPIRY_BUFFER: Duration = Duration::from_secs(45);

#[derive(Clone)]
struct CachedToken {
    token: String,
    expires_at: SystemTime,
}

impl CachedToken {
    fn new(token: String, expires_at: SystemTime) -> Self {
        Self { token, expires_at }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    user: String,
    host: String,
    port: u16,
}

impl From<&Address> for CacheKey {
    fn from(addr: &Address) -> Self {
        Self {
            user: addr.user.clone(),
            host: addr.host.clone(),
            port: addr.port,
        }
    }
}

/// Global per-address token cache for external identity providers
/// (RDS IAM, Azure Workload Identity, ...).
///
/// # Design
///
/// Accessed exclusively through [`TokenCache::global()`], which returns a
/// `&'static` reference — no cloning, no `Arc`, no allocation on every call.
///
/// The cache is kept warm by the pool monitor's token refresh loop, which
/// calls [`TokenCache::set`] proactively before each token expires and
/// [`TokenCache::evict`] when a refresh fails. Callers (the auth layer) use
/// [`TokenCache::get_or_fetch`], which:
///
/// - Returns immediately when any token is cached (valid or stale). Stale
///   tokens are acceptable for the brief window while the monitor is fetching
///   a replacement; the server will reject the connection if the token has
///   truly expired, which is the fallback signal to retry.
/// - Blocks exactly once on a cold miss (first connection before the monitor
///   has primed the cache, or after an eviction following a failed refresh).
pub struct TokenCache {
    inner: Mutex<HashMap<CacheKey, CachedToken>>,
}

impl TokenCache {
    fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the single global instance.
    pub fn global() -> &'static TokenCache {
        static INSTANCE: Lazy<TokenCache> = Lazy::new(TokenCache::new);
        &INSTANCE
    }

    /// When the cached token for `addr` expires, if one exists.
    ///
    /// Useful for introspection and testing. The monitor should prefer
    /// [`refresh_at`] for scheduling — it applies [`EXPIRY_BUFFER`]
    /// automatically.
    pub fn expires_at(&self, addr: &Address) -> Option<SystemTime> {
        self.inner
            .lock()
            .get(&CacheKey::from(addr))
            .map(|c| c.expires_at)
    }

    /// How long the monitor should sleep before waking up to refresh the
    /// token for `addr`.
    ///
    /// Returns the time until `expires_at - EXPIRY_BUFFER`. If no token is
    /// cached, the token expires sooner than the buffer, or the refresh
    /// instant is already in the past, returns [`Duration::ZERO`] so the
    /// monitor fires immediately.
    pub fn refresh_in(&self, addr: &Address) -> Duration {
        let Some(expires_at) = self
            .inner
            .lock()
            .get(&CacheKey::from(addr))
            .map(|c| c.expires_at)
        else {
            return Duration::ZERO; // cold start or eviction — fetch immediately
        };

        // If the token is already expired or expires within the buffer,
        // fetch immediately.
        expires_at
            .checked_sub(EXPIRY_BUFFER)
            .and_then(|refresh_at| refresh_at.duration_since(SystemTime::now()).ok())
            .unwrap_or(Duration::ZERO)
    }

    /// Store a freshly fetched token for `addr`.
    ///
    /// Called by the monitor after a successful proactive refresh.
    pub fn set(&self, addr: &Address, token: String, expires_at: SystemTime) {
        self.inner
            .lock()
            .insert(CacheKey::from(addr), CachedToken::new(token, expires_at));
    }

    /// Remove the cached token for `addr`.
    ///
    /// Called by the monitor when a refresh fails, so the next
    /// [`get_or_fetch`] blocks on a fresh fetch rather than handing out
    /// an expired token indefinitely.
    pub fn evict(&self, addr: &Address) {
        self.inner.lock().remove(&CacheKey::from(addr));
    }

    /// Return the cached token for `addr` if one exists, or call `fetcher`
    /// to obtain one on a cold miss.
    ///
    /// Always returns immediately when a token is present (valid or stale).
    /// Blocks only on a true cold miss.
    pub async fn get_or_fetch<F, Fut>(&self, addr: &Address, fetcher: F) -> Result<String, Error>
    where
        F: Fn(Address) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(String, SystemTime), Error>>,
    {
        if let Some(cached) = self.inner.lock().get(&CacheKey::from(addr)).cloned() {
            return Ok(cached.token);
        }

        // Cold miss — block once to prime the cache.
        // After this the monitor's refresh loop takes over.
        let (token, expires_at) = fetcher(addr.clone()).await?;
        self.set(addr, token.clone(), expires_at);
        Ok(token)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Each test uses a unique port to avoid cross-test interference
    /// in the global cache.
    fn addr(port: u16) -> Address {
        Address {
            host: "token-cache-test.internal".into(),
            port,
            user: "test_user".into(),
            ..Default::default()
        }
    }

    fn future_expiry(secs: u64) -> SystemTime {
        SystemTime::now() + Duration::from_secs(secs)
    }

    fn past_expiry(secs: u64) -> SystemTime {
        SystemTime::now() - Duration::from_secs(secs)
    }

    fn cache() -> &'static TokenCache {
        TokenCache::global()
    }

    // ── expires_at ───────────────────────────────────────────────────────────

    #[test]
    fn expires_at_returns_none_when_absent() {
        let a = addr(9900);
        cache().evict(&a);
        assert!(cache().expires_at(&a).is_none());
    }

    #[test]
    fn expires_at_returns_stored_expiry_after_set() {
        let a = addr(9901);
        let expiry = future_expiry(3600);
        cache().set(&a, "tok".into(), expiry);
        assert_eq!(cache().expires_at(&a).unwrap(), expiry);
        cache().evict(&a);
    }

    // ── refresh_in ───────────────────────────────────────────────────────────

    #[test]
    fn refresh_in_returns_zero_when_absent() {
        let a = addr(9914);
        cache().evict(&a);
        assert_eq!(cache().refresh_in(&a), Duration::ZERO);
    }

    #[test]
    fn refresh_in_returns_zero_for_already_expired_token() {
        let a = addr(9915);
        cache().set(&a, "tok".into(), past_expiry(60));
        assert_eq!(cache().refresh_in(&a), Duration::ZERO);
        cache().evict(&a);
    }

    #[test]
    fn refresh_in_returns_zero_when_expiry_within_buffer() {
        let a = addr(9916);
        // Expires in 10s — within EXPIRY_BUFFER (45s), so refresh_in
        // checked_sub underflows and returns Duration::ZERO.
        cache().set(&a, "tok".into(), future_expiry(10));
        assert_eq!(cache().refresh_in(&a), Duration::ZERO);
        cache().evict(&a);
    }

    #[test]
    fn refresh_in_returns_zero_when_expiry_before_unix_epoch_buffer() {
        let a = addr(9917);
        // expires_at so close to UNIX_EPOCH that checked_sub underflows.
        let expiry = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        cache().set(&a, "tok".into(), expiry);
        assert_eq!(cache().refresh_in(&a), Duration::ZERO);
        cache().evict(&a);
    }

    #[test]
    fn refresh_in_returns_duration_until_refresh_window() {
        let a = addr(9918);
        // Token expires in 3600s — refresh window opens at 3600 - 45 = 3555s.
        cache().set(&a, "tok".into(), future_expiry(3600));
        let d = cache().refresh_in(&a);
        // Allow a small margin for test execution time.
        assert!(d > Duration::from_secs(3550), "expected ~3555s, got {d:?}");
        assert!(d <= Duration::from_secs(3555), "expected ~3555s, got {d:?}");
        cache().evict(&a);
    }

    // ── set / evict ──────────────────────────────────────────────────────────

    #[test]
    fn set_overwrites_existing_entry() {
        let a = addr(9902);
        let first = future_expiry(1800);
        let second = future_expiry(3600);
        cache().set(&a, "first".into(), first);
        cache().set(&a, "second".into(), second);
        assert_eq!(cache().expires_at(&a).unwrap(), second);
        cache().evict(&a);
    }

    #[test]
    fn evict_removes_entry() {
        let a = addr(9903);
        cache().set(&a, "tok".into(), future_expiry(3600));
        assert!(cache().expires_at(&a).is_some());
        cache().evict(&a);
        assert!(cache().expires_at(&a).is_none());
    }

    #[test]
    fn evict_is_idempotent_when_absent() {
        let a = addr(9904);
        cache().evict(&a);
        cache().evict(&a); // must not panic
        assert!(cache().expires_at(&a).is_none());
    }

    // ── get_or_fetch ─────────────────────────────────────────────────────────

    #[test]
    fn cold_miss_calls_fetcher_and_caches_result() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let a = addr(9905);
        cache().evict(&a);

        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = calls.clone();

        let token = rt.block_on(cache().get_or_fetch(&a, move |_| {
            calls2.fetch_add(1, Ordering::SeqCst);
            async { Ok(("fresh".into(), future_expiry(3600))) }
        }));

        assert_eq!(token.unwrap(), "fresh");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(cache().expires_at(&a).is_some());
        cache().evict(&a);
    }

    #[test]
    fn warm_cache_returns_immediately_without_calling_fetcher() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let a = addr(9906);
        cache().set(&a, "cached".into(), future_expiry(3600));

        let token = rt.block_on(cache().get_or_fetch(&a, |_| async {
            panic!("fetcher must not be called on a cache hit");
            #[allow(unreachable_code)]
            Ok(("unreachable".into(), SystemTime::now()))
        }));

        assert_eq!(token.unwrap(), "cached");
        cache().evict(&a);
    }

    #[test]
    fn stale_token_returned_without_calling_fetcher() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let a = addr(9907);
        // Expired — stale but still returned; monitor handles refresh.
        cache().set(&a, "stale".into(), past_expiry(60));

        let token = rt.block_on(cache().get_or_fetch(&a, |_| async {
            panic!("fetcher must not be called for a stale cached token");
            #[allow(unreachable_code)]
            Ok(("unreachable".into(), SystemTime::now()))
        }));

        assert_eq!(token.unwrap(), "stale");
        cache().evict(&a);
    }

    #[test]
    fn second_call_hits_cache_populated_by_first_cold_miss() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let a = addr(9908);
        cache().evict(&a);

        rt.block_on(cache().get_or_fetch(&a, |_| async {
            Ok(("primed".into(), future_expiry(3600)))
        }))
        .unwrap();

        let token = rt.block_on(cache().get_or_fetch(&a, |_| async {
            panic!("fetcher must not be called on second call");
            #[allow(unreachable_code)]
            Ok(("unreachable".into(), SystemTime::now()))
        }));

        assert_eq!(token.unwrap(), "primed");
        cache().evict(&a);
    }

    #[test]
    fn cold_miss_after_evict_calls_fetcher_again() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let a = addr(9909);
        cache().evict(&a);

        rt.block_on(cache().get_or_fetch(&a, |_| async {
            Ok(("first".into(), future_expiry(3600)))
        }))
        .unwrap();

        cache().evict(&a);

        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = calls.clone();

        let token = rt.block_on(cache().get_or_fetch(&a, move |_| {
            calls2.fetch_add(1, Ordering::SeqCst);
            async { Ok(("second".into(), future_expiry(3600))) }
        }));

        assert_eq!(token.unwrap(), "second");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        cache().evict(&a);
    }

    #[test]
    fn fetch_error_on_cold_miss_returns_error_and_leaves_cache_empty() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let a = addr(9910);
        cache().evict(&a);

        let result = rt.block_on(cache().get_or_fetch(&a, |_| async {
            Err(Error::AzureWorkloadIdentityToken("fetch failed".into()))
        }));

        assert!(result.is_err());
        assert!(cache().expires_at(&a).is_none());
    }

    // ── cache key isolates by (user, host, port) ─────────────────────────────

    #[test]
    fn different_ports_are_independent_entries() {
        let a1 = addr(9911);
        let a2 = addr(9912);
        cache().evict(&a1);
        cache().evict(&a2);

        cache().set(&a1, "token-a1".into(), future_expiry(3600));

        let rt = tokio::runtime::Runtime::new().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = calls.clone();
        rt.block_on(cache().get_or_fetch(&a2, move |_| {
            calls2.fetch_add(1, Ordering::SeqCst);
            async { Ok(("token-a2".into(), future_expiry(3600))) }
        }))
        .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1, "a2 must fetch independently");
        cache().evict(&a1);
        cache().evict(&a2);
    }

    #[test]
    fn different_users_are_independent_entries() {
        let mut a1 = addr(9913);
        let mut a2 = addr(9913);
        a1.user = "alice".into();
        a2.user = "bob".into();
        cache().evict(&a1);
        cache().evict(&a2);

        cache().set(&a1, "alice-token".into(), future_expiry(3600));
        assert!(cache().expires_at(&a2).is_none());

        cache().evict(&a1);
        cache().evict(&a2);
    }
}