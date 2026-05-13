use std::hash::{Hash, Hasher};

use once_cell::sync::Lazy;
use regex::Regex;

use crate::{
    frontend::{ClientRequest, cache::CacheDecision},
    net::{FromBytes, Message, Parameters, Stream, ToBytes},
};

use tracing::debug;

use super::{policy, Cache};

static FORCE_CACHE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"pgdog_cache:\s*force_cache"#).unwrap()
});

pub struct CacheMiss {
    pub cache_key_hash: u64,
    pub ttl: u64,
}

pub enum CacheCheckResult {
    Hit {
        cached: Vec<u8>,
    },
    Miss(CacheMiss),
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
            let normalized_query = FORCE_CACHE_RE.replace(query.query(), "pgdog_cache: cache");
            normalized_query.hash(&mut hasher);
            hasher.finish()
        };

        let decision =
            policy::resolve(client_request, params, is_read, cache_key_hash, &self.stats).await;
        match decision {
            CacheDecision::Skip => Ok(CacheCheckResult::Passthrough),
            CacheDecision::ForceCache(ttl) => {
                self.stats.record_miss(cache_key_hash).await;
                Ok(CacheCheckResult::Miss(CacheMiss {
                    cache_key_hash,
                    ttl,
                }))
            },
            CacheDecision::Cache(ttl) => {
                match self.client.get(cache_key_hash).await {
                    Ok(Some(cached)) => {
                        self.stats.record_hit(cache_key_hash, cached.len()).await;
                        Ok(CacheCheckResult::Hit { cached })
                    }
                    Ok(None) => {
                        self.stats.record_miss(cache_key_hash).await;
                        Ok(CacheCheckResult::Miss(CacheMiss {
                            cache_key_hash,
                            ttl: ttl,
                        }))
                    }
                    Err(e) => {
                        debug!("Cache get error: {}", e);
                        Ok(CacheCheckResult::Passthrough)
                    }
                }
            },
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
        ttl: u64,
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
