use std::hash::{DefaultHasher, Hasher};

use crate::{
    frontend::client::query_engine::{cache::Cache, QueryEngineContext},
    net::{FromBytes, Message, ToBytes},
};

use tracing::debug;

use super::CachePolicyResolver;

pub enum CacheCheckResult {
    Hit {
        cached: Vec<u8>,
    },
    Miss {
        cache_key_hash: u64,
        ttl: Option<u64>,
    },
    Passthrough,
}

impl Cache {
    pub(super) async fn cache_check(
        &self,
        context: &mut QueryEngineContext<'_>,
    ) -> CacheCheckResult {
        let route = match context.client_request.route.as_ref() {
            Some(r) => r,
            None => return CacheCheckResult::Passthrough,
        };

        // Detect read-only status via the AST parser's route classification.
        // When caching is enabled, the query parser is auto-enabled.
        let is_read = route.is_read();
        if !is_read {
            return CacheCheckResult::Passthrough;
        }

        let query = match context.client_request.query() {
            Ok(Some(q)) => q,
            _ => return CacheCheckResult::Passthrough,
        };

        let db_hash = {
            let mut hasher = DefaultHasher::new();
            hasher.write(self.database.as_bytes());
            hasher.finish()
        };
        let cache_key_hash = pg_query::fingerprint(query.query())
            .expect("We're sure that query is correct if we've reached here.")
            .value
            .wrapping_add(db_hash);

        let cache_directive = self
            .policy_dispatcher
            .extract(query.query(), context.params);
        debug!(
            "cache_check: sql={}, db_config={:?}",
            query.query(),
            self.config
        );

        let decision = CachePolicyResolver::resolve(
            cache_directive,
            &self.config,
            is_read,
            cache_key_hash,
            &self.stats,
        )
        .await;

        if !decision.should_cache() {
            return CacheCheckResult::Passthrough;
        }

        match self.client.get(cache_key_hash).await {
            Ok(Some(cached)) => {
                self.stats.record_hit(cache_key_hash, cached.len()).await;
                CacheCheckResult::Hit { cached }
            }
            Ok(None) => {
                self.stats.record_miss(cache_key_hash).await;
                CacheCheckResult::Miss {
                    cache_key_hash,
                    ttl: decision.ttl(),
                }
            }
            Err(e) => {
                debug!("Cache get error: {}", e);
                CacheCheckResult::Passthrough
            }
        }
    }

    pub(super) async fn send_cached_response(
        &self,
        context: &mut QueryEngineContext<'_>,
        cached: Vec<u8>,
    ) -> Result<(), crate::frontend::Error> {
        let mut offset = 0;
        let len = cached.len();

        while offset < len {
            if offset + 5 > len {
                break;
            }

            let _code = cached[offset] as char;
            let msg_len = u32::from_be_bytes([
                cached[offset + 1],
                cached[offset + 2],
                cached[offset + 3],
                cached[offset + 4],
            ]) as usize;

            if msg_len < 4 || offset + 1 + msg_len > len {
                break;
            }

            let end = offset + 1 + msg_len;
            let msg_bytes: bytes::Bytes = cached[offset..end].to_vec().into();
            let msg = Message::from_bytes(msg_bytes)?;
            offset = end;

            context.stream.send_flush(&msg).await?;
        }

        Ok(())
    }

    pub(super) async fn cache_response(
        &self,
        cache_key_hash: u64,
        messages: Vec<Message>,
        ttl: Option<u64>,
    ) -> Result<(), ()> {
        if messages.is_empty() || !self.client.is_enabled() {
            return Ok(());
        }

        let mut buffer = Vec::new();
        for msg in &messages {
            match msg.to_bytes() {
                Ok(bytes) => buffer.extend_from_slice(&bytes),
                Err(e) => {
                    debug!("Failed to serialize message for caching: {}", e);
                    return Ok(());
                }
            }
        }

        if buffer.is_empty() {
            return Ok(());
        }

        if let Err(e) = self.client.set(cache_key_hash, &buffer, ttl).await {
            debug!("Failed to cache response: {}", e);
        }

        Ok(())
    }
}
