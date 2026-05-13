pub mod client;
pub mod context;
pub mod integration;
pub mod policy;
pub mod stats;

pub use client::CacheClient;
pub use integration::CacheCheckResult;
use pgdog_config::Cache as CacheConfig;
pub use policy::{
    CacheDecision, CachePolicyDispatcher, CachePolicyExtractor, CachePolicyResolver,
    CommentCacheExtractor, ParameterCacheExtractor,
};
pub use stats::QueryStatsTracker;
use tracing::debug;

use crate::frontend::client::query_engine::QueryEngineContext;

#[derive(Debug)]
pub struct Cache {
    client: CacheClient,
    stats: QueryStatsTracker,
    config: CacheConfig,
    database: String,
    policy_dispatcher: CachePolicyDispatcher,
}

impl Cache {
    pub fn new(cache_config: &CacheConfig, database: &str) -> Self {
        let mut dispatcher = CachePolicyDispatcher::new();
        dispatcher.add_extractor(Box::new(CommentCacheExtractor));
        dispatcher.add_extractor(Box::new(ParameterCacheExtractor::new()));

        Cache {
            client: CacheClient::new(cache_config),
            stats: QueryStatsTracker::default(),
            config: cache_config.clone(),
            database: database.to_string(),
            policy_dispatcher: dispatcher,
        }
    }

    pub async fn try_read_cache(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, crate::frontend::Error> {
        let cache_result = self.cache_check(context).await;

        match cache_result {
            CacheCheckResult::Hit { cached } => {
                debug!("Cache hit, serving from cache");
                self.send_cached_response(context, cached).await?;
                return Ok(true);
            }
            CacheCheckResult::Miss {
                cache_key_hash,
                ttl,
            } => {
                context.cache_context.cache_miss = Some((cache_key_hash, ttl));
                context.cache_context.response_buffer.clear();
                debug!("Cache miss for key hash: {}", cache_key_hash);
            }
            CacheCheckResult::Passthrough => {
                context.cache_context.cache_miss = None;
            }
        }

        return Ok(false);
    }

    /// Finalize caching by storing the response in Redis.
    pub async fn save_response_in_cache(&self, context: &mut QueryEngineContext<'_>,) {
        if let Some((cache_key, ttl)) = context.cache_context.cache_miss.take() {
            if !context.cache_context.response_buffer.is_empty() {
                let messages = std::mem::take(&mut context.cache_context.response_buffer);
                if let Err(e) = self.cache_response(cache_key, messages, ttl).await {
                    debug!("Failed to cache response: {:?}", e);
                }
            }
        }
    }
}