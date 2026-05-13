use std::hash::{Hash, Hasher};

use crate::{
    config::config,
    frontend::ClientRequest,
    net::{FromBytes, Message, Parameters, Stream, ToBytes},
};

use tracing::debug;

use super::{Cache, CachePolicyResolver};

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
        in_transaction: bool,
        client_request: &ClientRequest,
        params: &Parameters,
    ) -> Result<CacheCheckResult, crate::frontend::Error> {
        if in_transaction {
            return Ok(CacheCheckResult::Passthrough);
        }

        let route = match client_request.route.as_ref() {
            Some(r) => r,
            None => return Ok(CacheCheckResult::Passthrough),
        };

        // Detect read-only status via the AST parser's route classification.
        // When caching is enabled, the query parser is auto-enabled.
        let is_read = route.is_read();
        if !is_read {
            return Ok(CacheCheckResult::Passthrough);
        }

        let query = match client_request.query() {
            Ok(Some(q)) => q,
            _ => return Ok(CacheCheckResult::Passthrough),
        };

        let user = params.get_required("user")?;
        let database = params.get_default("database", user);
        let cache_key_hash = {
            let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
            database.hash(&mut hasher);
            query.query().hash(&mut hasher);
            hasher.finish()
        };

        let cache_directive = self.policy_dispatcher.extract(query.query(), params);
        let cache_config = &config().config.general.cache;

        debug!(
            "cache_check: sql={}, db_config={:?}",
            query.query(),
            cache_config
        );

        let decision = CachePolicyResolver::resolve(
            cache_directive,
            cache_config,
            is_read,
            cache_key_hash,
            &self.stats,
        )
        .await;

        if !decision.should_cache() {
            return Ok(CacheCheckResult::Passthrough);
        }

        match self.client.get(cache_key_hash).await {
            Ok(Some(cached)) => {
                self.stats.record_hit(cache_key_hash, cached.len()).await;
                Ok(CacheCheckResult::Hit { cached })
            }
            Ok(None) => {
                self.stats.record_miss(cache_key_hash).await;
                Ok(CacheCheckResult::Miss {
                    cache_key_hash,
                    ttl: decision.ttl(),
                })
            }
            Err(e) => {
                debug!("Cache get error: {}", e);
                Ok(CacheCheckResult::Passthrough)
            }
        }
    }

    pub(super) async fn send_cached_response(
        &self,
        stream: &mut Stream,
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

            stream.send_flush(&msg).await?;
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
