use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Display,
    ops::{Deref, DerefMut},
    str::FromStr,
};

use super::pooling::PoolerMode;

/// How aggressive the query parser should be in determining read vs. write queries.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/#read_write_strategy
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Copy, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReadWriteStrategy {
    /// Transactions are writes, standalone `SELECT` are reads (default).
    #[default]
    Conservative,
    /// Use first statement inside a transaction for determining query route.
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

/// Which strategy to use for load balancing read queries.
///
/// Note: See [load balancer](https://docs.pgdog.dev/features/load-balancer/) for more details.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/#load_balancing_strategy
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Copy, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalancingStrategy {
    /// Select a replica at random (default).
    #[default]
    Random,
    /// Distribute queries in a round-robin sequence.
    RoundRobin,
    /// Route to the replica with the fewest active connections.
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

/// How to handle the separation of read and write queries.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/#read_write_split
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Copy, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReadWriteSplit {
    /// Uses the primary database as well as the replicas to serve read queries (default).
    #[default]
    IncludePrimary,
    /// Sends all read queries to replicas, leaving the primary to serve only writes.
    ExcludePrimary,
    /// Sends reads to the primary only if one or more replicas have been banned.
    IncludePrimaryIfReplicaBanned,
}

impl FromStr for ReadWriteSplit {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace(['_', '-'], "").as_str() {
            "includeprimary" => Ok(Self::IncludePrimary),
            "excludeprimary" => Ok(Self::ExcludePrimary),
            "includeprimaryifreplicabanned" => Ok(Self::IncludePrimaryIfReplicaBanned),
            _ => Err(format!("Invalid read-write split: {}", s)),
        }
    }
}

impl Display for ReadWriteSplit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let display = match self {
            Self::ExcludePrimary => "exclude_primary",
            Self::IncludePrimary => "include_primary",
            Self::IncludePrimaryIfReplicaBanned => "include_primary_if_replica_banned",
        };

        write!(f, "{}", display)
    }
}

/// Database settings configure which databases PgDog is managing. This is a TOML list of hosts, ports, and other settings like database roles (primary or replica).
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/databases/
#[derive(
    Serialize, Deserialize, Debug, Clone, Default, PartialEq, Ord, PartialOrd, Eq, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct Database {
    /// Name of your database. Clients that connect to PgDog will need to use this name to refer to the database. For multiple entries that are part of the same cluster, use the same value.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#name
    pub name: String,
    /// Type of role this host performs in your database cluster. This can be `primary` for primary databases that serve writes (and reads), `replica` for PostgreSQL replicas that can only serve reads, or `auto` to let PgDog decide.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#role
    #[serde(default)]
    pub role: Role,
    /// IP address or DNS name of the machine where the PostgreSQL server is running.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#host
    pub host: String,
    /// The port PostgreSQL is running on. More often than not, this is going to be `5432`.
    ///
    /// _Default:_ `5432`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#port
    #[serde(default = "Database::port")]
    pub port: u16,
    /// The shard number for this database. Only required if your database contains more than one shard. Shard numbers start at 0.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#shard
    #[serde(default)]
    pub shard: usize,
    /// Name of the PostgreSQL database on the server PgDog will connect to. If not set, this defaults to `name`.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#database_name
    pub database_name: Option<String>,
    /// Name of the PostgreSQL user to connect with when creating backend connections from PgDog to Postgres. If not set, this defaults to `name` in users.toml.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#user
    pub user: Option<String>,
    /// Password to use when creating backend connections to PostgreSQL. If not set, this defaults to `password` in users.toml.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#password
    pub password: Option<String>,
    // Maximum number of connections to this database from this pooler.
    // #[serde(default = "Database::max_connections")]
    // pub max_connections: usize,
    /// Overrides the [`default_pool_size`](https://docs.pgdog.dev/configuration/pgdog.toml/general/#default_pool_size) setting. All connection pools for this database will open at most this many connections to Postgres.
    ///
    /// **Note:** We strongly recommend keeping this value well below the supported connections of the backend database(s) to allow connections for maintenance in high load scenarios.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#pool_size
    pub pool_size: Option<usize>,
    /// Overrides the `min_pool_size` setting. The connection pool will maintain at minimum this many connections.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#min_pool_size
    pub min_pool_size: Option<usize>,
    /// Overrides the `pooler_mode` setting. Connections to this database will use this connection pool mode.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#pooler_mode
    pub pooler_mode: Option<PoolerMode>,
    /// This setting configures the `statement_timeout` connection parameter on all connections to Postgres for this database.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#statement_timeout
    pub statement_timeout: Option<u64>,
    /// Overrides the `idle_timeout` setting. Idle server connections exceeding this timeout will be closed automatically.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#idle_timeout
    pub idle_timeout: Option<u64>,
    /// Sets the `default_transaction_read_only` connection parameter to `on` on all server connections to this database. Clients can still override it with `SET`.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#read_only
    pub read_only: Option<bool>,
    /// Overrides the `server_lifetime` setting. Server connections older than this will be closed when returned to the pool.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#server_lifetime
    pub server_lifetime: Option<u64>,
    /// Used for resharding only; this database will not serve regular traffic.
    #[serde(default)]
    pub resharding_only: bool,
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

/// Role a PostgreSQL server performs in a cluster.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/databases/#role
#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
    Default,
    PartialEq,
    Ord,
    PartialOrd,
    Eq,
    Hash,
    Copy,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Primary database that serves writes (and reads) (default).
    #[default]
    Primary,
    /// PostgreSQL replica that can only serve reads.
    Replica,
    /// Role is detected automatically by PgDog at runtime.
    Auto,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Primary => write!(f, "primary"),
            Self::Replica => write!(f, "replica"),
            Self::Auto => write!(f, "auto"),
        }
    }
}

impl FromStr for Role {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "primary" => Ok(Self::Primary),
            "replica" => Ok(Self::Replica),
            "auto" => Ok(Self::Auto),
            _ => Err(format!("Invalid role: {}", s)),
        }
    }
}

/// Database with a unique number, identifying it
/// in the config.
#[derive(Debug, Clone)]
pub struct EnumeratedDatabase {
    pub number: usize,
    pub database: Database,
}

impl Deref for EnumeratedDatabase {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        &self.database
    }
}

impl DerefMut for EnumeratedDatabase {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.database
    }
}
