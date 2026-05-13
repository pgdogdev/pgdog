pub mod client;
pub mod context;
pub mod integration;
pub mod policy;
pub mod stats;

pub use client::CacheClient;
pub use context::CacheContext;
pub use integration::CacheCheckResult;
pub use policy::CacheDecision;
pub use stats::QueryStatsTracker;

use once_cell::sync::Lazy;
use std::sync::Arc;
use tracing::debug;

use crate::{
    frontend::{ClientRequest, cache::integration::CacheMiss},
    net::{Parameters, Stream},
};

#[derive(Debug)]
pub struct Cache {
    client: CacheClient,
    stats: QueryStatsTracker,
}

static CACHE: Lazy<Arc<Cache>> = Lazy::new(|| Arc::new(Cache::new()));

pub fn cache() -> Arc<Cache> {
    CACHE.clone()
}

impl Cache {
    fn new() -> Self {
        Cache {
            client: CacheClient::new(),
            stats: QueryStatsTracker::default(),
        }
    }

    pub async fn try_read_cache(
        &self,
        cache_context: &mut CacheContext,
        in_transaction: bool,
        client_request: &ClientRequest,
        params: &Parameters,
        stream: &mut Stream,
    ) -> Result<bool, crate::frontend::Error> {
        let cache_result = self
            .cache_check(in_transaction, client_request, params)
            .await?;

        match cache_result {
            CacheCheckResult::Hit { cached } => {
                debug!("Cache hit, serving from cache");
                self.send_cached_response(stream, cached).await?;
                cache_context.reset();
                return Ok(true);
            }
            CacheCheckResult::Miss(cache_miss) => {
                debug!("Cache miss for key hash: {}", cache_miss.cache_key_hash);
                cache_context.cache_miss = Some(cache_miss);
                cache_context.response_buffer.clear();
                cache_context.had_error = false;
            }
            CacheCheckResult::Passthrough => {
                cache_context.reset();
            }
        }

        Ok(false)
    }

    /// Finalize caching by storing the response in Redis.
    pub async fn save_response_in_cache(&self, cache_context: &mut CacheContext) {
        if let Some(CacheMiss { cache_key_hash, ttl } ) = cache_context.cache_miss.take() {
            if !cache_context.had_error && !cache_context.response_buffer.is_empty() {
                let messages = std::mem::take(&mut cache_context.response_buffer);
                if let Err(e) = self.cache_response(cache_key_hash, messages, ttl).await {
                    debug!("Failed to cache response: {:?}", e);
                }
            }
        }
    }
}
