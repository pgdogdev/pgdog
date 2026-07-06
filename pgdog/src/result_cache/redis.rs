use std::{sync::Arc, time::Duration};

// Additions for encryption
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{AeadCore, Aes256Gcm, Key, Nonce};
use parking_lot::RwLock;
use aes_gcm::aead::OsRng;
use redis::AsyncCommands;
use regex::Regex;
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::{
    config::config,
    frontend::router::parser::OwnedTable,
    net::Parameters,
};

use super::{CacheableRequest, ResultCacheKey};

#[derive(Debug, Clone)]
pub struct ResultCacheConfig {
    pub enabled: bool,
    pub redis_url: String,
    pub expire_seconds: Option<u64>,
    pub max_entry_bytes: usize,
    pub encryption_key: Option<String>,
    pub key_prefix: String,
    pub safe_schema_list: Vec<Regex>,
    pub unsafe_schema_list: Vec<Regex>,
    pub safe_table_list: Vec<Regex>,
    pub unsafe_table_list: Vec<Regex>,
}

impl ResultCacheConfig {
    pub fn from_global_config() -> Option<Self> {
        let cfg = config();
        let rc = cfg.config.result_cache.clone()?;
        if !rc.enabled {
            return None;
        }

        Some(Self {
            enabled: rc.enabled,
            redis_url: rc.redis_url,
            // Apply a default TTL when not provided.
            expire_seconds: rc.expire_seconds.or(Some(30)),
            max_entry_bytes: rc.max_entry_bytes.unwrap_or(512 * 1024),
            encryption_key: rc.encryption_key,
            key_prefix: rc.key_prefix.unwrap_or_else(|| "pgdog:result_cache".to_string()),
            safe_schema_list: compile_list(&rc.cache_safe_schema_list),
            unsafe_schema_list: compile_list(&rc.cache_unsafe_schema_list),
            safe_table_list: compile_list(&rc.cache_safe_table_list),
            unsafe_table_list: compile_list(&rc.cache_unsafe_table_list),
        })
    }

    pub fn ttl(&self) -> Option<Duration> {
        self.expire_seconds.map(Duration::from_secs)
    }

    pub fn table_allowed(&self, table: &OwnedTable) -> bool {
        let schema = table.schema.as_deref().unwrap_or_default();
        let table_only = table.name.as_str();
        let schema_table = if schema.is_empty() {
            table_only.to_string()
        } else {
            format!("{}.{}", schema, table_only)
        };

        // Schema deny takes precedence.
        if !schema.is_empty() && matches_any(&self.unsafe_schema_list, schema) {
            return false;
        }

        // Table deny takes precedence.
        if matches_any(&self.unsafe_table_list, &schema_table)
            || matches_any(&self.unsafe_table_list, table_only)
        {
            return false;
        }

        // If allow lists are empty, allow by default.
        let schema_ok = self.safe_schema_list.is_empty()
            || (!schema.is_empty() && matches_any(&self.safe_schema_list, schema));
        let table_ok = self.safe_table_list.is_empty()
            || matches_any(&self.safe_table_list, &schema_table)
            || matches_any(&self.safe_table_list, table_only);

        schema_ok && table_ok
    }
}

#[derive(Debug)]
struct Shared {
    current_url: Option<String>,
    manager: Option<redis::aio::ConnectionManager>,
    prefix: String,
}

#[derive(Debug, Clone)]
pub struct RedisResultCache {
    shared: Arc<RwLock<Shared>>,
}

impl Default for RedisResultCache {
    fn default() -> Self {
        Self {
            shared: Arc::new(RwLock::new(Shared {
                current_url: None,
                manager: None,
                prefix: "pgdog:result_cache".into(),
            })),
        }
    }
}

impl RedisResultCache {
    pub async fn global() -> Option<Self> {
        let cfg = ResultCacheConfig::from_global_config()?;
        let cache = Self::default();
        cache.ensure_connected(&cfg).await?;
        Some(cache)
    }

    async fn ensure_connected(&self, cfg: &ResultCacheConfig) -> Option<()> {
        {
            let shared = self.shared.read();
            if shared.current_url.as_deref() == Some(cfg.redis_url.as_str())
                && shared.manager.is_some()
                && shared.prefix == cfg.key_prefix
            {
                return Some(());
            }
        }

        let client = redis::Client::open(cfg.redis_url.as_str()).ok()?;
        let manager = client.get_connection_manager().await.ok()?;
        let mut shared = self.shared.write();
        shared.current_url = Some(cfg.redis_url.clone());
        shared.manager = Some(manager);
        shared.prefix = cfg.key_prefix.clone();
        Some(())
    }

    fn tag_set_key(prefix: &str, db: &str, table: &OwnedTable) -> String {
        let schema = table.schema.as_deref().unwrap_or("_");
        format!("{prefix}:tbl:{db}:{schema}.{}", table.name)
    }

    pub async fn build_key(
        &self,
        db: &str,
        user: &str,
        params: &Parameters,
        req: &CacheableRequest,
    ) -> Option<ResultCacheKey> {
        let cfg = ResultCacheConfig::from_global_config()?;
        self.ensure_connected(&cfg).await?;

        // Filter by table/schema patterns.
        if !req.tables.is_empty() && req.tables.iter().any(|t| !cfg.table_allowed(t)) {
            return None;
        }

        // Session signature (conservative MVP):
        // include search_path and role-ish identity (user already included).
        let search_path = params.get("search_path").map(|v| v.to_string()).unwrap_or_default();
        let session_sig = format!("{:x}", md5::compute(format!("search_path={}", search_path)));

        // Query fingerprint: normalize to make cache key stable.
        // Include parameters to distinguish between queries with different values.
        let mut hasher = md5::Context::new();
        hasher.consume(req.query.as_bytes());
        for param in &req.parameters {
            hasher.consume(param);
        }
        let fingerprint = format!("{:x}", hasher.compute());

        let key = format!(
            "{}:v1:{}:{}:{}:{}:{}",
            cfg.key_prefix, db, user, session_sig, req.route_sig, fingerprint
        );

        Some(ResultCacheKey {
            redis_key: key,
            ttl: cfg.ttl(),
            max_entry_bytes: cfg.max_entry_bytes,
        })
    }

    pub async fn get(&self, key: &ResultCacheKey) -> Option<Vec<u8>> {
        let Some(cfg) = ResultCacheConfig::from_global_config() else {
            return None;
        };
        self.ensure_connected(&cfg).await?;

        let mut conn = {
            let mut shared = self.shared.write();
            match shared.manager.take() {
                Some(m) => m,
                None => return None,
            }
        };

        let res: redis::RedisResult<Vec<u8>> = conn.get(&key.redis_key).await;
        if let Ok(ref bytes) = res {
            if !bytes.is_empty() {
                if let Some(ttl) = key.ttl {
                    let _: redis::RedisResult<()> = conn.expire(&key.redis_key, ttl.as_secs() as i64).await;
                }
            }
        }

        let mut shared = self.shared.write();
        shared.manager = Some(conn);

        match res {
            Ok(bytes) if !bytes.is_empty() => {
                if let Some(enc_key) = &cfg.encryption_key {
                    // Decrypt the value
                    match decrypt(enc_key, &bytes) {
                        Some(plaintext) => Some(plaintext),
                        None => {
                            warn!(
                                "result_cache: failed to decrypt value for key: {}",
                                key.redis_key
                            );
                            // Treat as a cache miss.
                            None
                        }
                    }
                } else {
                    // No encryption key, return as is
                    Some(bytes)
                }
            }
            Ok(_) => None,
            Err(err) => {
                crate::stats::ResultCache::redis_error();
                warn!("result_cache get failed: {}", err);
                None
            }
        }
    }

    pub async fn set(&self, key: &ResultCacheKey, payload: &[u8]) {
        let Some(cfg) = ResultCacheConfig::from_global_config() else {
            return;
        };

        if payload.is_empty() {
            return;
        }

        // Encrypt payload if key is provided
        let final_payload = if let Some(enc_key) = &cfg.encryption_key {
            match encrypt(enc_key, payload) {
                Some(encrypted) => encrypted,
                None => {
                    warn!(
                        "result_cache: failed to encrypt value for key: {}",
                        key.redis_key
                    );
                    return; // Don't cache if encryption fails
                }
            }
        } else {
            payload.to_vec()
        };

        if final_payload.len() > key.max_entry_bytes {
            return;
        }

        if self.ensure_connected(&cfg).await.is_none() {
            return;
        }

        let mut conn = {
            let mut shared = self.shared.write();
            match shared.manager.take() {
                Some(m) => m,
                None => return,
            }
        };

        let res: redis::RedisResult<()> = match key.ttl {
            Some(ttl) if ttl > Duration::ZERO => {
                conn.set_ex(&key.redis_key, final_payload.as_slice(), ttl.as_secs() as u64)
                    .await
            }
            _ => conn.set(&key.redis_key, final_payload.as_slice()).await,
        };

        let mut shared = self.shared.write();
        shared.manager = Some(conn);

        if let Err(err) = res {
            crate::stats::ResultCache::redis_error();
            warn!("result_cache set failed: {}", err);
        }
    }

    pub async fn set_with_table_tags(
        &self,
        db: &str,
        key: &ResultCacheKey,
        payload: &[u8],
        tables: &[OwnedTable],
    ) {
        let Some(cfg) = ResultCacheConfig::from_global_config() else {
            return;
        };

        if tables.iter().any(|t| !cfg.table_allowed(t)) {
            return;
        }

        self.set(key, payload).await;

        if payload.is_empty() {
            return;
        }

        if self.ensure_connected(&cfg).await.is_none() {
            return;
        }

        let ttl = key.ttl.unwrap_or_else(|| Duration::from_secs(30));
        let set_ttl_secs = (ttl.as_secs().saturating_mul(2)).max(60);

        let mut conn = {
            let mut shared = self.shared.write();
            match shared.manager.take() {
                Some(m) => m,
                None => return,
            }
        };

        for table in tables {
            let set_key = Self::tag_set_key(&cfg.key_prefix, db, table);
            let _: redis::RedisResult<()> = conn.sadd(&set_key, &key.redis_key).await;
            let _: redis::RedisResult<()> = conn.expire(&set_key, set_ttl_secs as i64).await;
        }

        let mut shared = self.shared.write();
        shared.manager = Some(conn);
    }

    pub async fn invalidate_tables(&self, db: &str, tables: &[OwnedTable]) {
        let Some(cfg) = ResultCacheConfig::from_global_config() else {
            return;
        };
        if self.ensure_connected(&cfg).await.is_none() {
            return;
        }

        let mut conn = {
            let mut shared = self.shared.write();
            match shared.manager.take() {
                Some(m) => m,
                None => return,
            }
        };

        for table in tables {
            let set_key = Self::tag_set_key(&cfg.key_prefix, db, table);
            let keys: redis::RedisResult<Vec<String>> = conn.smembers(&set_key).await;
            match keys {
                Ok(keys) => {
                    if !keys.is_empty() {
                        let _: redis::RedisResult<()> = conn.del(keys).await;
                    }
                    let _: redis::RedisResult<()> = conn.del(&set_key).await;
                }
                Err(err) => {
                    crate::stats::ResultCache::redis_error();
                    warn!("result_cache invalidate failed: {}", err);
                }
            }
        }

        let mut shared = self.shared.write();
        shared.manager = Some(conn);
    }
}

fn get_cipher(key_str: &str) -> Aes256Gcm {
    // Use SHA-256 to derive a 32-byte key from the user-provided string.
    let mut hasher = Sha256::new();
    hasher.update(key_str.as_bytes());
    let binding = hasher.finalize();
    let key = Key::<Aes256Gcm>::from_slice(&binding);
    Aes256Gcm::new(key)
}

fn encrypt(key_str: &str, plaintext: &[u8]) -> Option<Vec<u8>> {
    let cipher = get_cipher(key_str);
    // 96-bits (12 bytes) is the standard nonce size for AES-GCM.
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    match cipher.encrypt(&nonce, plaintext) {
        Ok(ciphertext) => {
            let mut result = nonce.to_vec();
            result.extend_from_slice(&ciphertext);
            Some(result)
        }
        Err(_) => None,
    }
}

fn decrypt(key_str: &str, ciphertext_with_nonce: &[u8]) -> Option<Vec<u8>> {
    const NONCE_SIZE: usize = 12;
    if ciphertext_with_nonce.len() < NONCE_SIZE {
        return None; // Not long enough to contain a nonce
    }
    let cipher = get_cipher(key_str);
    let (nonce_bytes, ciphertext) = ciphertext_with_nonce.split_at(NONCE_SIZE);
    let nonce = Nonce::from_slice(nonce_bytes);
    cipher.decrypt(nonce, ciphertext).ok()
}

fn compile_list(patterns: &[String]) -> Vec<Regex> {
    patterns
        .iter()
        .filter_map(|p| Regex::new(p).ok())
        .collect()
}

fn matches_any(patterns: &[Regex], value: &str) -> bool {
    patterns.iter().any(|re| re.is_match(value))
}
