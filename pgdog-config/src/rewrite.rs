use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Controls what PgDog does when encountering a query that would require a rewrite.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/rewrite/
#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, JsonSchema,
)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum RewriteMode {
    /// Forward the query unchanged.
    Ignore,
    /// Return an error to the client (default).
    #[default]
    Error,
    /// Automatically rewrite the query and execute it.
    Rewrite,
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

/// Controls PgDog's automatic SQL rewrites for sharded databases. It affects sharding key updates and multi-tuple inserts.
///
/// **Note:** Consider enabling [two-phase commit](https://docs.pgdog.dev/features/sharding/2pc/) when either feature is set to `rewrite`. Without it, rewrites are committed shard-by-shard and can leave partial changes if a transaction fails.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/rewrite/
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Rewrite {
    /// Enables/disables the query rewrite engine.
    ///
    /// _Default:_ `false`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/rewrite/#enabled
    #[serde(default)]
    pub enabled: bool,

    /// Behavior for `UPDATE` statements changing sharding keys: `error` rejects, `rewrite` migrates rows between shards, `ignore` forwards unchanged.
    ///
    /// _Default:_ `error`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/rewrite/#shard_key
    #[serde(default = "Rewrite::default_shard_key")]
    pub shard_key: RewriteMode,

    /// Behavior for multi-row `INSERT` on sharded tables: `error` rejects, `rewrite` distributes rows to their shards, `ignore` forwards unchanged.
    ///
    /// _Default:_ `error`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/rewrite/#split_inserts
    #[serde(default = "Rewrite::default_split_inserts")]
    pub split_inserts: RewriteMode,

    /// Behavior for `INSERT` missing a `BIGINT` primary key: `error` rejects, `rewrite` auto-injects `pgdog.unique_id()`, `ignore` allows without modification.
    ///
    /// _Default:_ `ignore`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/rewrite/#primary_key
    #[serde(default = "Rewrite::default_primary_key")]
    pub primary_key: RewriteMode,
}

impl Default for Rewrite {
    fn default() -> Self {
        Self {
            enabled: false,
            shard_key: Self::default_shard_key(),
            split_inserts: Self::default_split_inserts(),
            primary_key: Self::default_primary_key(),
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

    const fn default_primary_key() -> RewriteMode {
        RewriteMode::Ignore
    }
}
