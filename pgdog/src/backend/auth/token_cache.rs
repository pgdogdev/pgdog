use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::time::{Duration, SystemTime};
use tracing::warn;

use crate::backend::{pool::Address, Error};

/// How early to consider a token expired to avoid edge-cases at the boundary.
pub(super) const EXPIRY_BUFFER: Duration = Duration::from_secs(45);

#[derive(Clone)]
pub(super) struct CachedToken {
    pub(super) token: String,
    expires_at: SystemTime,
}

impl CachedToken {
    pub(super) fn new(token: String, expires_at: SystemTime) -> Self {
        Self { token, expires_at }
    }

    fn is_valid(&self) -> bool {
        SystemTime::now()
            .checked_add(EXPIRY_BUFFER)
            .map(|t| t < self.expires_at)
            .unwrap_or(false)
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct CacheKey {
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

struct Cache {
    tokens: HashMap<CacheKey, CachedToken>,
    refreshing: HashSet<CacheKey>,
}

impl Cache {
    fn new() -> Self {
        Self {
            tokens: HashMap::new(),
            refreshing: HashSet::new(),
        }
    }
}

static TOKEN_CACHE: Lazy<Mutex<Cache>> = Lazy::new(|| Mutex::new(Cache::new()));

enum GetResult {
    Miss,
    Hit(String),
}

fn get(key: &CacheKey) -> GetResult {
    match TOKEN_CACHE.lock().tokens.get(key) {
        Some(c) if c.is_valid() => GetResult::Hit(c.token.clone()),
        _ => GetResult::Miss,
    }
}

fn set(key: CacheKey, token: String, expires_at: SystemTime) {
    TOKEN_CACHE
        .lock()
        .tokens
        .insert(key, CachedToken::new(token, expires_at));
}

fn claim_refresh(key: &CacheKey) -> bool {
    TOKEN_CACHE.lock().refreshing.insert(key.clone())
}

fn release_refresh(key: &CacheKey) {
    TOKEN_CACHE.lock().refreshing.remove(key);
}

#[cfg(test)]
pub(super) fn insert_test_token(key: CacheKey, token: CachedToken) {
    TOKEN_CACHE.lock().tokens.insert(key, token);
}

/// Get a cached token for `addr`, or fetch and cache one using `fetcher`.
///
/// On a miss, fetches, caches the result, and spawns a single
/// background task that refreshes the token shortly before each expiry.
pub(super) async fn get_or_fetch<F, Fut>(addr: &Address, fetcher: F) -> Result<String, Error>
where
    F: Fn(Address) -> Fut + Send + Sync + Copy + 'static,
    Fut: Future<Output = Result<(String, SystemTime), Error>> + Send + 'static,
{
    let key = CacheKey::from(addr);

    if let GetResult::Hit(token) = get(&key) {
        return Ok(token);
    }

    let (token, expires_at) = fetcher(addr.clone()).await?;
    set(key, token.clone(), expires_at);
    spawn_refresh_task(addr.clone(), expires_at, fetcher);
    Ok(token)
}

fn spawn_refresh_task<F, Fut>(addr: Address, initial_expires_at: SystemTime, fetcher: F)
where
    F: Fn(Address) -> Fut + Send + Sync + Copy + 'static,
    Fut: Future<Output = Result<(String, SystemTime), Error>> + Send + 'static,
{
    let key = CacheKey::from(&addr);
    if !claim_refresh(&key) {
        return;
    }

    tokio::spawn(async move {
        let mut expires_at = initial_expires_at;
        loop {
            tokio::time::sleep(
                expires_at
                    .duration_since(SystemTime::now())
                    .unwrap_or_default()
                    .saturating_sub(EXPIRY_BUFFER),
            )
            .await;

            match fetcher(addr.clone()).await {
                Ok((token, new_expires_at)) => {
                    set(CacheKey::from(&addr), token, new_expires_at);
                    expires_at = new_expires_at;
                }
                Err(e) => {
                    // Release the slot so the next cache miss can re-spawn.
                    warn!("Background token refresh failed, stopping: {e}");
                    release_refresh(&key);
                    break;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_before_expiry() {
        let t = CachedToken::new("tok".into(), SystemTime::now() + Duration::from_secs(120));
        assert!(t.is_valid());
    }

    #[test]
    fn invalid_within_buffer() {
        let t = CachedToken::new("tok".into(), SystemTime::now() + Duration::from_secs(10));
        assert!(!t.is_valid());
    }

    #[test]
    fn invalid_after_expiry() {
        let t = CachedToken::new("tok".into(), SystemTime::now() - Duration::from_secs(1));
        assert!(!t.is_valid());
    }

    #[test]
    fn claim_refresh_only_succeeds_once() {
        let key = CacheKey {
            user: "u".into(),
            host: "h".into(),
            port: 1,
        };
        TOKEN_CACHE.lock().refreshing.remove(&key); // clean slate
        assert!(claim_refresh(&key));
        assert!(!claim_refresh(&key));
        release_refresh(&key);
        assert!(claim_refresh(&key));
        release_refresh(&key);
    }
}
