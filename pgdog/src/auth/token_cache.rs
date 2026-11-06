//! Token cache that validates a token we received
//! from the client is actually valid with whatever mechanism
//! we authenticate to Postgres with.
//!
//! This is effectively passthrough auth for RDS IAM, Azure Workload Identity, etc.
//!

use lru::LruCache;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use pgdog_config::ServerAuth;
use rand::seq::{IndexedRandom, IteratorRandom};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::sync::OnceCell;
use tracing::error;

use super::Error;
use crate::tasks;
use crate::{backend::pool::Connection, config::config};

/// Client token cache.
///
/// Tokens expire on lookup according to their provider's expected lifetime.
pub static AUTH_TOKEN_CACHE: Lazy<AuthTokenCache> = Lazy::new(|| {
    let cache = AuthTokenCache::default();
    let background = cache.clone();

    tasks::spawn("auth token cache", async move {
        let shutdown = tasks::shutdown_signal();
        tokio::select! {
            _ = background.eviction_loop() => {}
            _ = shutdown.cancelled() => {}
        }
    });

    cache
});

#[derive(Debug)]
struct Entry {
    // Hardcoded to documented expiration time in the future.
    expires_at: SystemTime,
    // Only the initializing caller validates; waiters share its result.
    validation: OnceCell<bool>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Ord, PartialOrd)]
struct KeyInner {
    user: String,
    database: String,
    token: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Ord, PartialOrd)]
struct Key {
    inner: Arc<KeyInner>,
}

impl Key {
    fn new(user: &str, database: &str, token: &str) -> Self {
        Self {
            inner: Arc::new(KeyInner {
                user: user.to_owned(),
                database: database.to_owned(),
                token: token.to_owned(),
            }),
        }
    }
}

impl Entry {
    fn expired(&self) -> bool {
        self.expires_at <= SystemTime::now()
    }

    fn valid(&self) -> bool {
        self.validation.get() == Some(&true) && !self.expired()
    }

    fn new(expires_at: SystemTime) -> Self {
        Self {
            expires_at,
            validation: OnceCell::new(),
        }
    }
}

#[derive(Debug, Default)]
struct Stats {
    evictions: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

#[derive(Clone, Debug)]
pub(crate) struct AuthTokenCache {
    // Lookups use peek to preserve FIFO eviction order.
    inner: Arc<Mutex<LruCache<Key, Arc<Entry>>>>,
    stats: Arc<Stats>,
}

impl Default for AuthTokenCache {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(LruCache::unbounded())),
            stats: Arc::new(Stats::default()),
        }
    }
}

fn expires_at(server_auth: ServerAuth) -> SystemTime {
    match server_auth {
        ServerAuth::RdsIam => crate::backend::auth::rds_iam::expires_at(),
        ServerAuth::AzureWorkloadIdentity => {
            crate::backend::auth::azure_workload_identity::hardcoded_expires_at()
        }
        _ => SystemTime::now() + Duration::from_hours(24), // Not expected, but used in tests.
    }
}

impl AuthTokenCache {
    /// Check a token against the external token provider, e.g., RDS.
    ///
    /// Shares initialization of each token so a connection storm from clients
    /// only creates one connection to the actual database.
    ///
    /// # Arguments
    ///
    /// - `user`: user name from users.toml
    /// - `database`: database name from pgdog.toml
    /// - `token`: the token we got from the client.
    ///
    pub(crate) async fn check(
        &self,
        user: &str,
        database: &str,
        token: &str,
    ) -> Result<bool, Error> {
        let key = Key::new(user, database, token);

        if self.get_and_check(&key) {
            return Ok(true);
        }

        // Get a random pool from any of the shards.
        //
        // Invariant: all shards have the same IAM permissions.
        //
        let conn = Connection::new(user, database, false)?;
        let shard = conn
            .cluster()?
            .shards()
            .choose(&mut rand::rng())
            .expect("to have at least one shard");
        let pool = shard
            .pool_iter()
            .choose(&mut rand::rng())
            .expect("to have at least one pool");
        let server_auth = pool.addr().server_auth;

        let entry = self.insert(&key, server_auth);

        // Validate until we get an auth error.
        // Connection errors are bubbled up.
        // Multiple clients can attempt validation if server
        // returns a non-auth error.
        if let Err(err) = entry
            .validation
            .get_or_try_init(|| async {
                let valid = pool.validate_token(token).await?;
                Ok::<bool, crate::backend::Error>(valid)
            })
            .await
        {
            error!("client token validation error: {}", err);
            return Ok(false);
        }

        Ok(entry.valid())
    }

    fn insert(&self, key: &Key, server_auth: ServerAuth) -> Arc<Entry> {
        let expires_at = expires_at(server_auth);
        let mut guard = self.inner.lock();

        // Clients should not be sending the same token
        // that's expired but we can't trust them.
        let exists = guard.peek(key);

        if let Some(exists) = exists {
            exists.clone()
        } else {
            let value = Arc::new(Entry::new(expires_at));
            guard.put(key.clone(), value.clone());

            value
        }
    }

    fn get_and_check(&self, key: &Key) -> bool {
        let mut guard = self.inner.lock();
        let (ok, exists, expired) = guard
            .peek(key)
            .map(|entry| (entry.valid(), true, entry.expired()))
            .unwrap_or_default();

        if ok {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            // Remove expired key and let the cache re-validate it
            // against Postgres. This is to protect against
            // mismatched cache expiration setting.
            if exists && expired {
                guard.pop(key);
            }
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
        }

        ok
    }

    fn run_eviction(&self, capacity: usize) {
        let mut guard = self.inner.lock();

        while guard.len() > capacity {
            if guard.pop_lru().is_some() {
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    async fn eviction_loop(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(333)).await;
            let capacity = config().config.general.auth_token_cache_size as usize;
            self.run_eviction(capacity);
        }
    }

    /// Get stats from cache.
    pub(crate) fn stats(&self) -> TokenCacheStats {
        TokenCacheStats {
            entries: self.inner.lock().len() as u64,
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            misses: self.stats.misses.load(Ordering::Relaxed),
            hits: self.stats.hits.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TokenCacheStats {
    pub(crate) entries: u64,
    pub(crate) evictions: u64,
    pub(crate) misses: u64,
    pub(crate) hits: u64,
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_expired_key_is_validated_again() {
        crate::logger();
        crate::config::load_test();
        let cache = AuthTokenCache::default();
        let key = Key::new("pgdog", "pgdog", "pgdog");

        assert!(
            cache
                .check("pgdog", "pgdog", "pgdog")
                .await
                .expect("initial token validation should succeed")
        );

        let expired = {
            let mut guard = cache.inner.lock();
            let entry = guard.peek_mut(&key).expect("token should be cached");
            Arc::get_mut(entry)
                .expect("cache should be the only entry owner")
                .expires_at = SystemTime::now() - Duration::from_secs(1);
            entry.clone()
        };
        assert!(expired.expired());

        assert!(
            cache
                .check("pgdog", "pgdog", "pgdog")
                .await
                .expect("expired token should be revalidated")
        );
        {
            let guard = cache.inner.lock();
            let refreshed = guard.peek(&key).expect("token should be cached again");
            assert!(!Arc::ptr_eq(&expired, refreshed));
            assert_eq!(refreshed.validation.get(), Some(&true));
            assert!(!refreshed.expired());
        }

        // A stale queue entry must not evict the newly validated token.
        cache.run_eviction(1);
        assert!(cache.get_and_check(&key));
    }

    #[tokio::test]
    async fn test_cache() {
        crate::logger();
        crate::config::load_test();
        let valid = AUTH_TOKEN_CACHE
            .check("pgdog", "pgdog", "pgdog")
            .await
            .unwrap();
        assert!(valid);

        let err = AUTH_TOKEN_CACHE
            .check("doesn't_exist", "pgdog", "pgdog")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Backend(_)));
    }
}
