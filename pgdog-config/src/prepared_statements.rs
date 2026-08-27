//! Prepared statement settings of one connection pool.

use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::{DurationMilliSeconds, serde_as};

use crate::PreparedStatementsLevel;

/// How a server connection handles prepared statements.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, JsonSchema)]
pub struct PreparedStatementsConfig {
    /// Which statements PgDog keeps prepared on the connection.
    pub level: PreparedStatementsLevel,
    /// Maximum prepared statements per connection.
    pub limit: usize,
    /// How long a statement can keep a cached plan. `None` never expires.
    #[serde_as(as = "Option<DurationMilliSeconds>")]
    pub ttl: Option<Duration>,
    /// Random spread applied to `ttl`, per statement.
    #[serde_as(as = "DurationMilliSeconds")]
    pub ttl_jitter: Duration,
}

impl Default for PreparedStatementsConfig {
    fn default() -> Self {
        Self {
            level: PreparedStatementsLevel::default(),
            limit: i64::MAX as usize,
            ttl: None,
            ttl_jitter: Duration::ZERO,
        }
    }
}
