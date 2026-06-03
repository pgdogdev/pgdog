pub mod redis;

pub use redis::RedisCacheStorage;

use async_trait::async_trait;

use crate::config::{cache::CacheBackend, config};

/// Errors returned by cache storage backends.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Redis {cmd} error for key {key}: {err}")]
    RedisError {
        cmd: &'static str,
        key: u64,
        err: fred::error::RedisError,
    },
    #[error("Connection failed: {0}")]
    ConnectionFailed(&'static str),
    #[error("Cache miss for key {0}")]
    CacheMiss(u64),
}

/// Abstract cache storage backend.
///
/// Implementations must be `Send + Sync` so they can be held behind
/// something like `Arc<Box<dyn CacheStorage>>` and shared across async tasks.
#[async_trait]
pub trait CacheStorage: Send + Sync {
    /// Fetch cached bytes for `key`. Returns [`Error::CacheMiss`] when the
    /// key is absent (not an error condition — used for control flow).
    async fn get(&self, key: u64) -> Result<Vec<u8>, Error>;

    /// Store `value` under `key` with a `ttl` in seconds.
    async fn set(&self, key: u64, value: &[u8], ttl: u64) -> Result<(), Error>;

    /// Returns `true` when the backend is configured and enabled.
    fn is_enabled(&self) -> bool;

    /// Returns `true` if cache config has changed (used for hotswap detection).
    ///
    /// This method should check only those parameters that require a storage rebuild and
    /// that are specific to the storage, e.g. `Config::backend` and storage's own settings.
    fn is_actual(&self) -> bool;
}

/// Construct the appropriate storage backend from the current config.
pub async fn build_storage() -> Option<Box<dyn CacheStorage>> {
    let cfg = &config().config.general.cache;
    if !cfg.enabled {
        return None;
    }
    match cfg.backend {
        CacheBackend::Redis => RedisCacheStorage::new(cfg)
            .await
            .map(|s| Box::new(s) as Box<dyn CacheStorage>),
    }
}
