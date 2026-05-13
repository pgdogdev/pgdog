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

/// Redis cache configuration for a database.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Cache {
    /// Whether to enable caching for this database.
    /// 
    /// _Default:_ `false`
    #[serde(default = "Cache::enabled")]
    pub enabled: bool,
    /// Cache policy: no_cache or cache.
    ///
    /// _Default:_ `no_cache`
    #[serde(default = "Cache::policy")]
    pub policy: CachePolicy,
    /// Default TTL in seconds for cached queries.
    ///
    /// _Default:_ `300`
    #[serde(default = "Cache::ttl")]
    pub ttl: u64,
    /// Redis connection URL.
    ///
    /// _Default:_ `redis://localhost:6379`
    #[serde(default = "Cache::redis_url")]
    pub redis_url: String,
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
            redis_url: Self::redis_url(),
            max_result_size: Self::max_result_size(),
        }
    }
}

impl Cache {
    fn enabled() -> bool {
        false
    }

    fn policy() -> CachePolicy {
        Default::default()
    }

    fn ttl() -> u64 {
        300
    }

    fn redis_url() -> String {
        "redis://localhost:6379".to_string()
    }

    fn max_result_size() -> usize {
        0
    }
}