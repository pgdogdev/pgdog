use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResultCache {
    /// Enable Redis/RESP result cache.
    #[serde(default)]
    pub enabled: bool,

    /// Redis connection URL, e.g. redis://127.0.0.1:6379
    #[serde(default)]
    pub redis_url: String,

    /// Optional expiration time for cached entries (seconds).
    pub expire_seconds: Option<u64>,

    /// Maximum entry size to cache (bytes). If not set, PgDog uses a safe default.
    pub max_entry_bytes: Option<usize>,

    /// Key prefix in Redis.
    pub key_prefix: Option<String>,

    /// Optional allow-list of schemas whose tables can be cached.
    ///
    /// Each entry is treated as a regular expression.
    #[serde(default)]
    pub cache_safe_schema_list: Vec<String>,

    /// Optional deny-list of schemas whose tables should never be cached.
    ///
    /// Takes precedence over `cache_safe_schema_list`.
    /// Each entry is treated as a regular expression.
    #[serde(default)]
    pub cache_unsafe_schema_list: Vec<String>,

    /// Optional allow-list of tables whose results can be cached.
    ///
    /// Each entry is treated as a regular expression and matched against
    /// either `table` or `schema.table` when schema is present.
    #[serde(default)]
    pub cache_safe_table_list: Vec<String>,

    /// Optional deny-list of tables whose results should never be cached.
    ///
    /// Takes precedence over `cache_safe_table_list`.
    /// Each entry is treated as a regular expression and matched against
    /// either `table` or `schema.table` when schema is present.
    #[serde(default)]
    pub cache_unsafe_table_list: Vec<String>,
}

