use serde::{Deserialize, Serialize};
use std::str::FromStr;

use super::pooling::PoolerMode;

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Copy)]
#[serde(rename_all = "snake_case")]
pub enum ReadWriteStrategy {
    #[default]
    Conservative,
    Aggressive,
}

impl FromStr for ReadWriteStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "conservative" => Ok(Self::Conservative),
            "aggressive" => Ok(Self::Aggressive),
            _ => Err(format!("Invalid read-write strategy: {}", s)),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Copy)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalancingStrategy {
    #[default]
    Random,
    RoundRobin,
    LeastActiveConnections,
}

impl FromStr for LoadBalancingStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace(['_', '-'], "").as_str() {
            "random" => Ok(Self::Random),
            "roundrobin" => Ok(Self::RoundRobin),
            "leastactiveconnections" => Ok(Self::LeastActiveConnections),
            _ => Err(format!("Invalid load balancing strategy: {}", s)),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Copy)]
#[serde(rename_all = "snake_case")]
pub enum ReadWriteSplit {
    #[default]
    IncludePrimary,
    ExcludePrimary,
}

impl FromStr for ReadWriteSplit {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace(['_', '-'], "").as_str() {
            "includeprimary" => Ok(Self::IncludePrimary),
            "excludeprimary" => Ok(Self::ExcludePrimary),
            _ => Err(format!("Invalid read-write split: {}", s)),
        }
    }
}

/// Database server proxied by pgDog.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Ord, PartialOrd, Eq)]
#[serde(deny_unknown_fields)]
pub struct Database {
    /// Database name visible to the clients.
    pub name: String,
    /// Database role, e.g. primary.
    #[serde(default)]
    pub role: Role,
    /// Database host or IP address, e.g. 127.0.0.1.
    pub host: String,
    /// Database port, e.g. 5432.
    #[serde(default = "Database::port")]
    pub port: u16,
    /// Shard.
    #[serde(default)]
    pub shard: usize,
    /// PostgreSQL database name, e.g. "postgres".
    pub database_name: Option<String>,
    /// Use this user to connect to the database, overriding the userlist.
    pub user: Option<String>,
    /// Use this password to login, overriding the userlist.
    pub password: Option<String>,
    // Maximum number of connections to this database from this pooler.
    // #[serde(default = "Database::max_connections")]
    // pub max_connections: usize,
    /// Pool size for this database pools, overriding `default_pool_size`.
    pub pool_size: Option<usize>,
    /// Minimum pool size for this database pools, overriding `min_pool_size`.
    pub min_pool_size: Option<usize>,
    /// Pooler mode.
    pub pooler_mode: Option<PoolerMode>,
    /// Statement timeout.
    pub statement_timeout: Option<u64>,
    /// Idle timeout.
    pub idle_timeout: Option<u64>,
    /// Read-only mode.
    pub read_only: Option<bool>,
    /// GSSAPI keytab for this specific backend server.
    pub gssapi_keytab: Option<String>,
    /// GSSAPI principal for this specific backend server.
    pub gssapi_principal: Option<String>,
}

impl Database {
    #[allow(dead_code)]
    fn max_connections() -> usize {
        usize::MAX
    }

    fn port() -> u16 {
        5432
    }
}

#[derive(
    Serialize, Deserialize, Debug, Clone, Default, PartialEq, Ord, PartialOrd, Eq, Hash, Copy,
)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    #[default]
    Primary,
    Replica,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Primary => write!(f, "primary"),
            Self::Replica => write!(f, "replica"),
        }
    }
}
