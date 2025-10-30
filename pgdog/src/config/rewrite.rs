use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RewriteMode {
    Error,
    Rewrite,
    Ignore,
}

impl Default for RewriteMode {
    fn default() -> Self {
        Self::Error
    }
}

impl fmt::Display for RewriteMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            RewriteMode::Error => "error",
            RewriteMode::Rewrite => "rewrite",
            RewriteMode::Ignore => "ignore",
        };
        f.write_str(value)
    }
}

impl FromStr for RewriteMode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "error" => Ok(RewriteMode::Error),
            "rewrite" => Ok(RewriteMode::Rewrite),
            "ignore" => Ok(RewriteMode::Ignore),
            _ => Err(()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Rewrite {
    /// Global rewrite toggle. When disabled, rewrite-specific features remain
    /// inactive, even if individual policies request rewriting.
    #[serde(default)]
    pub enabled: bool,
    /// Policy for handling shard-key updates.
    #[serde(default = "Rewrite::default_shard_key")]
    pub shard_key: RewriteMode,
    /// Policy for handling multi-row INSERT statements that target sharded tables.
    #[serde(default = "Rewrite::default_split_inserts")]
    pub split_inserts: RewriteMode,
}

impl Default for Rewrite {
    fn default() -> Self {
        Self {
            enabled: false,
            shard_key: Self::default_shard_key(),
            split_inserts: Self::default_split_inserts(),
        }
    }
}

impl Rewrite {
    const fn default_shard_key() -> RewriteMode {
        RewriteMode::Error
    }

    const fn default_split_inserts() -> RewriteMode {
        RewriteMode::Error
    }
}
