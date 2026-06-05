pub mod context;
pub mod directive;
pub mod integration;
pub mod storage;

pub use context::CacheContext;
pub use directive::CacheDirective;
pub use integration::CacheCheckResult;
pub use storage::{CacheStorage, RedisCacheStorage};

use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};
use tracing::debug;

use crate::{
    config::config,
    frontend::{
        cache::{integration::CacheMiss, storage::build_storage},
        ClientRequest,
    },
    net::{Message, Parameters},
};

/// Wraps the active storage backend behind a tokio `RwLock` so it can be
/// hotswapped without restarting pgdog.
pub struct Cache {
    storage: RwLock<Option<Box<dyn CacheStorage>>>,
}

impl std::fmt::Debug for Cache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").field("storage", &"...").finish()
    }
}

static CACHE: OnceCell<Arc<Cache>> = OnceCell::const_new();

pub async fn cache() -> Arc<Cache> {
    CACHE
        .get_or_init(async || Arc::new(Cache::new().await))
        .await
        .clone()
}

impl Cache {
    async fn new() -> Self {
        let storage = build_storage().await;
        Cache {
            storage: RwLock::new(storage),
        }
    }

    /// Replace the storage backend if the config has changed (URL or backend type).
    ///
    /// Acquires the write lock only when a change is detected.
    async fn hotswap_if_needed(&self) {
        // Fast path: read-lock to check whether anything has changed.
        {
            let guard = self.storage.read().await;
            let cfg = &config().config.general.cache;
            let needs_swap = match guard.as_ref() {
                Some(s) => s.is_actual(),
                None => cfg.enabled,
            };
            if !needs_swap {
                return;
            }
        }

        // Slow path: write-lock, re-check and rebuild.
        let mut guard = self.storage.write().await;
        let cfg = &config().config.general.cache;
        let needs_swap = match guard.as_ref() {
            Some(s) => s.is_actual(),
            None => cfg.enabled,
        };

        if needs_swap {
            debug!("Cache storage config changed — rebuilding backend");
            *guard = build_storage().await;
        }
    }

    // ── public API ───────────────────────────────────────────────────────────

    /// Check the cache for a query response.
    ///
    /// On HIT returns `Ok(Some(messages))` — the caller is responsible for
    /// replaying these messages through the normal server-message pipeline.
    ///
    /// On MISS or PASSTHROUGH returns `Ok(None)` and updates `cache_context`
    /// so that the response can later be captured and stored via
    /// `save_response_in_cache`.
    pub async fn try_read_cache(
        &self,
        cache_context: &mut CacheContext,
        in_transaction: bool,
        client_request: &ClientRequest,
        params: &Parameters,
    ) -> Result<Option<Vec<Message>>, crate::frontend::Error> {
        self.hotswap_if_needed().await;

        let cache_result = self
            .cache_check(in_transaction, client_request, params)
            .await?;

        match cache_result {
            CacheCheckResult::Hit { cached } => {
                debug!("Cache hit, serving from cache");
                let messages = Self::deserialize_cached(cached);
                cache_context.reset();
                Ok(Some(messages))
            }
            CacheCheckResult::Miss(cache_miss) => {
                debug!("Cache miss for key hash: {}", cache_miss.key);
                cache_context.cache_miss = Some(cache_miss);
                cache_context.response_buffer.clear();
                cache_context.had_error = false;
                Ok(None)
            }
            CacheCheckResult::Passthrough => {
                cache_context.reset();
                Ok(None)
            }
        }
    }

    /// Finalize caching by storing the response in the active backend.
    pub async fn save_response_in_cache(&self, cache_context: &mut CacheContext) {
        self.hotswap_if_needed().await;

        if let Some(CacheMiss { key, ttl }) = cache_context.cache_miss.take() {
            if !cache_context.had_error && !cache_context.response_buffer.is_empty() {
                let messages = std::mem::take(&mut cache_context.response_buffer);
                self.cache_response(key, messages, ttl).await;
            }
        }
    }
}
