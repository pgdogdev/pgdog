use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// prepared statement support mode.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/#prepared_statements
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Copy, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PreparedStatements {
    /// Prepared statements are disabled.
    Disabled,
    /// Handles prepared statements sent normally using the extended protocol (default).
    #[default]
    Extended,
    /// Caches and rewrites unnamed prepared statements, useful for some legacy client drivers.
    ExtendedAnonymous,
    /// Enables support for rewriting prepared statements sent over the simple protocol.
    Full,
}

impl PreparedStatements {
    pub fn full(&self) -> bool {
        matches!(self, PreparedStatements::Full)
    }

    pub fn enabled(&self) -> bool {
        !matches!(self, PreparedStatements::Disabled)
    }

    pub fn rewrite_anonymous(&self) -> bool {
        matches!(self, PreparedStatements::ExtendedAnonymous)
    }
}

impl FromStr for PreparedStatements {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disabled" => Ok(Self::Disabled),
            "extended" => Ok(Self::Extended),
            "extended_anonymous" => Ok(Self::ExtendedAnonymous),
            "full" => Ok(Self::Full),
            _ => Err(format!("Invalid prepared statements mode: {}", s)),
        }
    }
}

/// connection pooling mode for database pools.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/#pooler_mode
#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq, Ord, PartialOrd, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum PoolerMode {
    /// Server connections are checked out only for the duration of a transaction (default).
    ///
    /// See [transaction mode](https://docs.pgdog.dev/features/transaction-mode/).
    #[default]
    Transaction,
    /// Each client connection maps to a dedicated server connection for the session duration.
    ///
    /// See [session mode](https://docs.pgdog.dev/features/session-mode/).
    Session,
    /// Server connections are checked out only for a single statement.
    Statement,
}

impl std::fmt::Display for PoolerMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transaction => write!(f, "transaction"),
            Self::Session => write!(f, "session"),
            Self::Statement => write!(f, "statement"),
        }
    }
}

impl FromStr for PoolerMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "transaction" => Ok(Self::Transaction),
            "session" => Ok(Self::Session),
            _ => Err(format!("Invalid pooler mode: {}", s)),
        }
    }
}

/// controls if server connections are recovered or dropped if a client abruptly disconnects.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/#connection_recovery
#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq, Ord, PartialOrd, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionRecovery {
    /// Attempt to recover the connection by rolling back any open transaction and resynchronizing (default).
    #[default]
    Recover,
    /// Attempt to `ROLLBACK` any unfinished transactions but do not attempt to resynchronize connections.
    RollbackOnly,
    /// Close connections without attempting recovery.
    Drop,
}

impl ConnectionRecovery {
    pub fn can_recover(&self) -> bool {
        matches!(self, ConnectionRecovery::Recover)
    }

    pub fn can_rollback(&self) -> bool {
        matches!(
            self,
            ConnectionRecovery::Recover | ConnectionRecovery::RollbackOnly
        )
    }
}

impl FromStr for ConnectionRecovery {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "recover" => Ok(Self::Recover),
            "rollbackonly" => Ok(Self::RollbackOnly),
            "drop" => Ok(Self::Drop),
            _ => Err(format!("Invalid pooler mode: {}", s)),
        }
    }
}
