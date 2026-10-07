//! Token cache that validates a token we received
//! from the client is actually valid with whatever mechanism
//! we authenticate to Postgres with.
//!
//! This is effectively passthrough auth for RDS IAM, Azure Workload Identity, etc.

use moka::sync::Cache;
use once_cell::sync::Lazy;
use rand::seq::{IndexedRandom, IteratorRandom};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{sync::Mutex, time::sleep};

use crate::backend::pool::Connection;
use crate::tasks::{shutdown_signal, spawn};

pub static AUTH_TOKEN_CACHE: Lazy<AuthTokenCache> = Lazy::new(|| {
    let cache = AuthTokenCache::default();
    let background = cache.clone();

    spawn("auth token cache", async move {
        let shutdown = shutdown_signal();
        loop {
            tokio::select! {
                _ = sleep(Duration::from_millis(333)) => background.evict(),
                _ = shutdown.cancelled() => break,
            }
        }
    });

    cache
});

#[derive(Debug, Clone)]
struct Entry {
    expires_at: SystemTime,
    lock: Arc<Mutex<()>>,
}

#[derive(Clone, Debug)]
pub(crate) struct AuthTokenCache {
    tokens: Cache<String, Entry>,
}

impl Default for AuthTokenCache {
    fn default() -> Self {
        Self {
            tokens: Cache::builder()
                .max_capacity(1_000)
                .support_invalidation_closures()
                .build(),
        }
    }
}

impl AuthTokenCache {
    fn evict(&self) {
        let now = SystemTime::now();
        self.tokens
            .invalidate_entries_if(move |_, value| value.expires_at <= now)
            .expect("invalidation closures are enabled");
    }

    /// Check a token against the external token provider, e.g., RDS.
    ///
    /// Takes a lock on the token so a connection storm from clients
    /// only creates one connection to the actual database.
    pub(crate) async fn check(
        &self,
        user: &str,
        database: &str,
        token: &str,
    ) -> Result<bool, crate::frontend::Error> {
        if self.tokens.contains_key(token) {
            return Ok(true);
        }

        let lock = {
            let entry = self.tokens.entry(token.to_owned()).or_insert(Entry {
                expires_at: crate::backend::auth::rds_iam::expires_at(),
                lock: Arc::new(Mutex::new(())),
            });
            entry.value().lock.clone()
        };

        let _guard = lock.lock().await;

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

        if let Ok(()) = pool.validate_token(token).await {
            Ok(true)
        } else {
            self.tokens.remove(token);
            Ok(false)
        }
    }
}
