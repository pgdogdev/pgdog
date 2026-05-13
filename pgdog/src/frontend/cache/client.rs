use fred::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

use crate::config::config;

const CACHE_KEY_PREFIX: &str = "pgdog:";

/// Timeout for individual Redis operations (GET/SET/init).
/// Safety net — should never fire in normal operation since the atomic flag gates all calls.
const REDIS_OPERATION_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct CacheClient {
    client: Option<RedisClient>,
    /// Master connection state flag. Set true only after PING succeeds
    /// on init or reconnect. Set false immediately on any error/timeout.
    redis_connected: Arc<AtomicBool>,
    /// Prevents spawning multiple reconnect tasks simultaneously.
    reconnecting: Arc<AtomicBool>,
}

impl std::fmt::Debug for CacheClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheClient")
            .field("client", &self.client.as_ref().map(|_| "..."))
            .field(
                "redis_connected",
                &self.redis_connected.load(Ordering::Relaxed),
            )
            .field("reconnecting", &self.reconnecting.load(Ordering::Relaxed))
            .finish()
    }
}

impl CacheClient {
    pub fn new() -> Self {
        let cache_config = &config().config.general.cache;

        if !cache_config.is_enabled() || cache_config.redis_url.is_none() {
            return Self {
                client: None,
                redis_connected: Arc::new(AtomicBool::new(false)),
                reconnecting: Arc::new(AtomicBool::new(false)),
            };
        }

        let url = cache_config.redis_url.as_ref().unwrap();
        let client_config = match RedisConfig::from_url(url) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to parse Redis URL: {}", e);
                return Self {
                    client: None,
                    redis_connected: Arc::new(AtomicBool::new(false)),
                    reconnecting: Arc::new(AtomicBool::new(false)),
                };
            }
        };

        let client = match Builder::from_config(client_config).build() {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to build Redis client: {}", e);
                return Self {
                    client: None,
                    redis_connected: Arc::new(AtomicBool::new(false)),
                    reconnecting: Arc::new(AtomicBool::new(false)),
                };
            }
        };

        Self {
            client: Some(client),
            redis_connected: Arc::new(AtomicBool::new(false)),
            reconnecting: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn ensure_connected(&self) -> bool {
        if self.redis_connected.load(Ordering::Acquire) {
            return true;
        }

        if self.reconnecting.load(Ordering::Relaxed) {
            return false;
        }

        if let Some(ref client) = self.client {
            match tokio::time::timeout(REDIS_OPERATION_TIMEOUT, client.init()).await {
                Ok(Ok(_)) => {
                    if Self::ping_client(client).await {
                        self.redis_connected.store(true, Ordering::Release);
                        info!("Connected to Redis");
                        return true;
                    } else {
                        debug!("Redis init returned OK but PING failed — Redis not ready");
                    }
                }
                Ok(Err(e)) => {
                    debug!("Redis init failed: {}", e);
                }
                Err(_) => {
                    error!("Redis init timed out");
                }
            }
        }
        false
    }

    async fn ping_client(client: &RedisClient) -> bool {
        match tokio::time::timeout(REDIS_OPERATION_TIMEOUT, client.ping::<String>()).await {
            Ok(Ok(resp)) => {
                info!("Redis PING succeeded: {}", resp);
                true
            }
            Ok(Err(e)) => {
                debug!("Redis PING failed: {}", e);
                false
            }
            Err(_) => {
                debug!("Redis PING timed out");
                false
            }
        }
    }

    fn spawn_reconnect(&self) {
        if self
            .reconnecting
            .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed)
            .is_err()
        {
            debug!("Redis reconnect task already running, skipping");
            return;
        }

        let Some(ref client) = self.client else {
            error!("Redis reconnect: no client available");
            self.reconnecting.store(false, Ordering::Release);
            return;
        };

        let client = client.clone();
        let redis_connected = self.redis_connected.clone();
        let reconnecting = self.reconnecting.clone();

        tokio::spawn(async move {
            info!("Redis reconnect task started");
            let mut attempt = 0;
            loop {
                attempt += 1;
                debug!("Redis reconnect attempt #{}", attempt);

                let init_ok =
                    match tokio::time::timeout(REDIS_OPERATION_TIMEOUT, client.init()).await {
                        Ok(Ok(_)) => true,
                        Ok(Err(_)) | Err(_) => false,
                    };

                if init_ok || Self::ping_client(&client).await {
                    redis_connected.store(true, Ordering::Release);
                    reconnecting.store(false, Ordering::Release);
                    info!("Redis reconnected successfully");
                    return;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        info!("Spawning Redis reconnect task");
    }

    fn mark_disconnected(&self) {
        self.redis_connected.store(false, Ordering::Release);
        self.spawn_reconnect();
    }

    pub fn is_connected(&self) -> bool {
        self.redis_connected.load(Ordering::Relaxed)
    }

    pub(crate) async fn get(&self, key: u64) -> Result<Option<Vec<u8>>, Error> {
        if !self.ensure_connected().await {
            if !self.is_connected() {
                self.spawn_reconnect();
                return Err(Error::ConnectionFailed(
                    "Redis disconnected, reconnecting in background".to_string(),
                ));
            }
            return Err(Error::ConnectionFailed("Redis not connected".to_string()));
        }

        let Some(ref client) = self.client else {
            return Ok(None);
        };

        let full_key = format!("{}{}", CACHE_KEY_PREFIX, key);
        let val = match tokio::time::timeout(
            REDIS_OPERATION_TIMEOUT,
            client.get::<RedisValue, _>(full_key),
        )
        .await
        {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                debug!("Redis GET error for key {}: {}", key, e);
                self.mark_disconnected();
                return Err(Error::RedisError(e.to_string()));
            }
            Err(_) => {
                error!("Redis GET timed out for key {}", key);
                self.mark_disconnected();
                return Err(Error::ConnectionFailed("Redis GET timed out".to_string()));
            }
        };

        if val.is_null() {
            debug!("Cache miss for key {}", key);
            Ok(None)
        } else if let Some(bytes) = val.into_bytes() {
            debug!("Cache hit for key {}", key);
            Ok(Some(bytes.to_vec()))
        } else {
            debug!("Redis GET value not bytes for key {}", key);
            Ok(None)
        }
    }

    pub(crate) async fn set(&self, key: u64, value: &[u8], ttl: Option<u64>) -> Result<(), Error> {
        if !self.ensure_connected().await {
            if !self.is_connected() {
                self.spawn_reconnect();
                return Err(Error::ConnectionFailed(
                    "Redis disconnected, reconnecting in background".to_string(),
                ));
            }
            return Err(Error::ConnectionFailed("Redis not connected".to_string()));
        }

        let Some(ref client) = self.client else {
            return Ok(());
        };

        let full_key = format!("{}{}", CACHE_KEY_PREFIX, key);

        let cache_config = &config().config.general.cache;

        if let Some(max_size) = cache_config.max_result_size() {
            if value.len() > max_size {
                debug!(
                    "Skipping cache for key {}: size {} exceeds max {}",
                    key,
                    value.len(),
                    max_size
                );
                return Ok(());
            }
        }

        let ttl_seconds = ttl.unwrap_or_else(|| cache_config.ttl()) as i64;

        match tokio::time::timeout(
            REDIS_OPERATION_TIMEOUT,
            client.set::<(), _, _>(
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
            Ok(Err(e)) => {
                debug!("Redis SET error for key {}: {}", key, e);
                self.mark_disconnected();
                Err(Error::RedisError(e.to_string()))
            }
            Err(_) => {
                error!("Redis SET timed out for key {}", key);
                self.mark_disconnected();
                Err(Error::ConnectionFailed("Redis SET timed out".to_string()))
            }
        }
    }

    pub fn is_enabled(&self) -> bool {
        let cache_config = &config().config.general.cache;
        self.client.is_some() && cache_config.is_enabled()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Redis error: {0}")]
    RedisError(String),
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
}
