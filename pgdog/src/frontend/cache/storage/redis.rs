use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use fred::prelude::*;
use pgdog_config::CacheBackend;
use tracing::{debug, error, info};

use crate::config::{cache::Cache as CacheConfig, config};

use super::{CacheStorage, Error};

/// Timeout for individual Redis operations (GET/SET/ping).
const REDIS_OPERATION_TIMEOUT: Duration = Duration::from_secs(2);
/// Max time between reconnection attempts
const MAX_REDIS_RECONNECTION_PERIOD: Duration = Duration::from_secs(5);

/// Redis implementation of [`CacheStorage`].
///
/// Connection is established in a background task spawned from [`RedisCacheStorage::new`].
/// All operations return immediately if the connection is not yet ready — `get` returns
/// [`Error::ConnectionFailed`] (triggering a cache-miss path) and `set` is silently dropped.
///
/// At most one reconnect task runs at any time, enforced by a CAS on `reconnecting`.
pub struct RedisCacheStorage {
    client: RedisClient,
    /// Cache config.
    config: CacheConfig,
    /// Guards against spawning multiple concurrent reconnect tasks.
    reconnecting: Arc<AtomicBool>,
}

impl std::fmt::Debug for RedisCacheStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisCacheStorage")
            .field("config", &self.config)
            .field("reconnecting", &self.reconnecting.load(Ordering::Relaxed))
            .finish()
    }
}

impl RedisCacheStorage {
    /// Build a new storage instance for `url` and immediately start a background
    /// connection task.  Returns `None` when the URL cannot be parsed.
    pub fn new(config: &CacheConfig) -> Option<Self> {
        let client_config = match RedisConfig::from_url(&config.redis.url) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to parse Redis URL '{}': {}", config.redis.url, e);
                return None;
            }
        };

        let client = match Builder::from_config(client_config).build() {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to build Redis client: {}", e);
                return None;
            }
        };

        let reconnecting = Arc::new(AtomicBool::new(true)); // treat initial connect as "reconnecting"

        let storage = Self {
            client,
            config: config.clone(),
            reconnecting,
        };

        // Fire-and-forget initial connection.
        storage.spawn_connect_task();

        Some(storage)
    }

    // ── internal helpers ────────────────────────────────────────────────────

    /// Spawn the (re)connect background loop. Uses a CAS to ensure only one
    /// task is ever running at a time.
    fn spawn_connect_task(&self) {
        let client = self.client.clone();
        let reconnecting = self.reconnecting.clone();

        tokio::spawn(async move {
            info!("Redis connect task started");
            let mut attempt = 0u32;

            loop {
                attempt += 1;
                debug!("Redis connect attempt #{}", attempt);

                let init_ok =
                    match tokio::time::timeout(REDIS_OPERATION_TIMEOUT, client.init()).await {
                        Ok(Ok(_)) => true,
                        Ok(Err(e)) => {
                            debug!("Redis init error: {}", e);
                            false
                        }
                        Err(_) => {
                            debug!("Redis init timed out");
                            false
                        }
                    };

                if init_ok {
                    reconnecting.store(false, Ordering::Release);
                    info!("Redis connected (attempt #{})", attempt);
                    return;
                }

                // Exponential backoff
                tokio::time::sleep(
                    const { Duration::from_millis(5) }
                        .saturating_mul(1u32 << attempt.min(10))
                        .min(MAX_REDIS_RECONNECTION_PERIOD),
                )
                .await;
            }
        });
    }

    /// Mark the reconnecting as true and spawn a reconnect task if one is not
    /// already running.
    fn reconnect(&self) {
        if self
            .reconnecting
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            self.spawn_connect_task();
        } else {
            debug!("Redis reconnect task already running");
        }
    }
}

#[async_trait]
impl CacheStorage for RedisCacheStorage {
    async fn get(&self, key: u64) -> Result<Vec<u8>, Error> {
        if self.reconnecting.load(Ordering::Acquire) {
            return Err(Error::ConnectionFailed("Redis not connected"));
        }

        let full_key = format!("{}{}", self.config.redis.cache_key_prefix, key);

        let redis_result = tokio::time::timeout(
            REDIS_OPERATION_TIMEOUT,
            self.client.get::<RedisValue, _>(full_key),
        )
        .await;
        let val = match redis_result {
            Ok(Ok(v)) => v,
            Ok(Err(err)) => {
                self.reconnect();
                return Err(Error::RedisError {
                    cmd: "GET",
                    key,
                    err,
                });
            }
            Err(_) => {
                self.reconnect();
                return Err(Error::ConnectionFailed("Redis GET timed out"));
            }
        };

        match val.into_bytes() {
            Some(bytes) => {
                debug!("Cache hit for key {}", key);
                Ok(bytes.to_vec())
            }
            None => Err(Error::CacheMiss(key)),
        }
    }

    async fn set(&self, key: u64, value: &[u8], ttl: u64) -> Result<(), Error> {
        if self.reconnecting.load(Ordering::Acquire) {
            return Err(Error::ConnectionFailed("Redis not connected"));
        }

        let max_result_size = config().config.general.cache.max_result_size;
        if max_result_size != 0 && value.len() > max_result_size {
            debug!(
                "Skipping cache for key {}: size {} exceeds max {}",
                key,
                value.len(),
                max_result_size
            );
            return Ok(());
        }

        let full_key = format!("{}{}", self.config.redis.cache_key_prefix, key);
        let ttl_seconds = ttl as i64;

        match tokio::time::timeout(
            REDIS_OPERATION_TIMEOUT,
            self.client.set::<(), _, _>(
                full_key,
                value,
                Some(Expiration::EX(ttl_seconds)),
                None,
                false,
            ),
        )
        .await
        {
            Ok(Ok(_)) => {
                debug!("Cached key {} with TTL {}s", key, ttl_seconds);
                Ok(())
            }
            Ok(Err(err)) => {
                self.reconnect();
                Err(Error::RedisError {
                    cmd: "SET",
                    key,
                    err,
                })
            }
            Err(_) => {
                self.reconnect();
                Err(Error::ConnectionFailed("Redis SET timed out"))
            }
        }
    }

    fn is_enabled(&self) -> bool {
        config().config.general.cache.enabled
    }

    fn has_config_changed(&self) -> bool {
        let new_config = &config().config.general.cache;
        new_config.backend != CacheBackend::Redis
            || self.config.redis.url != new_config.redis.url
    }
}

// Avoid shallow copy
impl Clone for RedisCacheStorage {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone_new(),
            config: self.config.clone(),
            reconnecting: Arc::new(AtomicBool::new(false)),
        }
    }
}
