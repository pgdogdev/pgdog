use std::num::NonZeroU64;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Cache policy.
#[derive(
    Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Copy, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CachePolicy {
    /// Never cache queries for this database.
    #[default]
    NoCache,
    /// Always cache read queries.
    Cache,
}

impl std::str::FromStr for CachePolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "no_cache" => Ok(Self::NoCache),
            "cache" => Ok(Self::Cache),
            _ => Err(format!("Invalid cache policy: {}", s)),
        }
    }
}

impl std::fmt::Display for CachePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let display = match self {
            Self::NoCache => "no_cache",
            Self::Cache => "cache",
        };
        write!(f, "{}", display)
    }
}

/// Cache storage backend discriminator.
#[derive(
    Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Copy, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CacheBackend {
    /// Redis backend (default).
    #[default]
    Redis,
}

/// Redis-specific cache backend configuration.
///
/// Corresponds to the `[general.cache.redis]` TOML section.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    /// Redis connection URL.
    ///
    /// _Default:_ `redis://localhost:6379`
    #[serde(default = "RedisConfig::url")]
    pub url: String,

    /// Key prefix prepended to every cache key stored in Redis.
    ///
    /// _Default:_ `pgdog:`
    #[serde(default = "RedisConfig::cache_key_prefix")]
    pub cache_key_prefix: String,

    /// Timeout in milliseconds for individual Redis operations (GET/SET/ping).
    ///
    /// _Default:_ `2000`
    #[serde(default = "RedisConfig::operation_timeout")]
    pub operation_timeout: NonZeroU64,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: Self::url(),
            cache_key_prefix: Self::cache_key_prefix(),
            operation_timeout: Self::operation_timeout(),
        }
    }
}

impl RedisConfig {
    fn url() -> String {
        "redis://localhost:6379".to_string()
    }

    fn cache_key_prefix() -> String {
        "pgdog:".to_string()
    }

    fn operation_timeout() -> NonZeroU64 {
        NonZeroU64::new(2000).expect("2000 is non-zero")
    }
}

/// Cache configuration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Cache {
    /// Whether to enable caching.
    ///
    /// _Default:_ `false`
    #[serde(default = "Cache::enabled")]
    pub enabled: bool,

    /// Cache policy: `no_cache` or `cache`.
    ///
    /// _Default:_ `no_cache`
    #[serde(default = "Cache::policy")]
    pub policy: CachePolicy,

    /// Default TTL in seconds for cached queries.
    ///
    /// _Default:_ `300`
    #[serde(default = "Cache::ttl")]
    pub ttl: u64,

    /// Which storage backend to use.
    ///
    /// _Default:_ `redis`
    #[serde(default = "Cache::backend")]
    pub backend: CacheBackend,

    /// Redis backend configuration.
    ///
    /// Only read when `backend = "redis"`.
    #[serde(default)]
    pub redis: RedisConfig,

    /// Maximum result size in bytes to cache (0 = unlimited).
    ///
    /// _Default:_ `0`
    #[serde(default = "Cache::max_result_size")]
    pub max_result_size: usize,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            enabled: Self::enabled(),
            policy: Self::policy(),
            ttl: Self::ttl(),
            backend: Self::backend(),
            redis: RedisConfig::default(),
            max_result_size: Self::max_result_size(),
        }
    }
}

impl Cache {
    fn enabled() -> bool {
        false
    }

    fn policy() -> CachePolicy {
        CachePolicy::default()
    }

    fn ttl() -> u64 {
        300
    }

    fn backend() -> CacheBackend {
        CacheBackend::default()
    }

    fn max_result_size() -> usize {
        0
    }
}
