use std::hash::{Hash, Hasher};

use crate::{
    config::{cache::CachePolicy, config},
    frontend::{
        cache::{
            directive::{self, CacheMode},
            hashing::compute_cache_key_hash,
            storage::Error as CacheStorageError,
            Cache,
        },
        ClientRequest,
    },
    net::{Message, Parameters, ToBytes},
};

use tracing::warn;

pub struct CacheMiss {
    pub key: u64,
    pub ttl: u64,
}

pub enum CacheCheckResult {
    Hit { cached: Vec<u8> },
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
        if in_transaction || !client_request.is_executable() {
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

        let directive = directive::resolve(client_request, params);
        let cache_config = &config().config.general.cache;

        let ttl = directive.ttl_seconds.unwrap_or(cache_config.ttl);
        let mode = match directive.mode {
            Some(mode) => mode,
            None => match cache_config.policy {
                CachePolicy::NoCache => CacheMode::NoCache,
                CachePolicy::Cache => CacheMode::Cache,
            },
        };

        if mode == CacheMode::NoCache {
            return Ok(CacheCheckResult::Passthrough);
        }

        let key = match directive.key {
            Some(key) => {
                let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
                key.hash(&mut hasher);
                hasher.finish()
            }
            None => {
                let user = params.get_required("user")?;
                let database = params.get_default("database", user);
                let bind = client_request.parameters()?;
                compute_cache_key_hash(database, query.query(), bind)
            }
        };

        if mode == CacheMode::ForceCache {
            return Ok(CacheCheckResult::Miss(CacheMiss { key, ttl }));
        }

        let guard = self.storage.read().await;
        let storage = match guard.as_ref() {
            Some(storage) => storage,
            None => return Ok(CacheCheckResult::Passthrough),
        };
        match storage.get(key).await {
            Ok(cached) => Ok(CacheCheckResult::Hit { cached }),
            Err(CacheStorageError::CacheMiss(_)) => {
                Ok(CacheCheckResult::Miss(CacheMiss { key, ttl }))
            }
            Err(e) => {
                warn!("{}", e);
                Ok(CacheCheckResult::Passthrough)
            }
        }
    }

    pub(super) async fn cache_response(&self, key: u64, messages: Vec<Message>, ttl: u64) {
        let guard = self.storage.read().await;
        let storage = match guard.as_ref() {
            Some(s) if s.is_enabled() => s,
            _ => return,
        };

        if messages.is_empty() {
            return;
        }

        let mut buffer = Vec::new();
        for msg in &messages {
            match msg.to_bytes() {
                Ok(bytes) => buffer.extend_from_slice(&bytes),
                Err(e) => {
                    warn!("Failed to serialize message for caching: {}", e);
                    return;
                }
            }
        }

        if buffer.is_empty() {
            return;
        }

        if let Err(e) = storage.set(key, &buffer, ttl).await {
            warn!("{}", e);
        }
    }
}
