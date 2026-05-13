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
    /// Dynamically decide based on Redis memory and query stats.
    Auto,
}

impl std::str::FromStr for CachePolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "no_cache" => Ok(Self::NoCache),
            "cache" => Ok(Self::Cache),
            "auto" => Ok(Self::Auto),
            _ => Err(format!("Invalid cache policy: {}", s)),
        }
    }
}

impl std::fmt::Display for CachePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let display = match self {
            Self::NoCache => "no_cache",
            Self::Cache => "cache",
            Self::Auto => "auto",
        };
        write!(f, "{}", display)
    }
}

/// Redis cache configuration for a database.
#[derive(
    Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct Cache {
    /// Whether to enable caching for this database.
    pub enabled: Option<bool>,
    /// Cache policy: no_cache, cache, or auto.
    pub policy: Option<CachePolicy>,
    /// Default TTL in seconds for cached queries.
    pub ttl: Option<u64>,
    /// Redis connection URL (e.g., redis://localhost:6379).
    pub redis_url: Option<String>,
    /// Maximum result size in bytes to cache (0 = unlimited).
    pub max_result_size: Option<usize>,
}

impl Cache {
    pub fn is_enabled(&self) -> bool {
        self.enabled.unwrap_or(false)
    }

    pub fn policy(&self) -> CachePolicy {
        self.policy.unwrap_or_default()
    }

    pub fn ttl(&self) -> u64 {
        self.ttl.unwrap_or(300)
    }

    pub fn max_result_size(&self) -> Option<usize> {
        self.max_result_size
    }
}
