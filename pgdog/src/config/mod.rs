//! Configuration.

pub mod convert;
pub mod error;
pub mod overrides;
pub mod url;

use error::Error;
pub use overrides::Overrides;

use std::collections::HashSet;
use std::fs::read_to_string;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, path::PathBuf};

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tracing::info;
use tracing::warn;

use crate::net::messages::Vector;
use crate::util::{human_duration_optional, random_string};

static CONFIG: Lazy<ArcSwap<ConfigAndUsers>> =
    Lazy::new(|| ArcSwap::from_pointee(ConfigAndUsers::default()));

// static LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Load configuration.
pub fn config() -> Arc<ConfigAndUsers> {
    CONFIG.load().clone()
}

/// Load the configuration file from disk.
pub fn load(config: &PathBuf, users: &PathBuf) -> Result<ConfigAndUsers, Error> {
    let config = ConfigAndUsers::load(config, users)?;
    set(config)
}

pub fn set(mut config: ConfigAndUsers) -> Result<ConfigAndUsers, Error> {
    config.config.check();
    for table in config.config.sharded_tables.iter_mut() {
        table.load_centroids()?;
    }
    CONFIG.store(Arc::new(config.clone()));
    Ok(config)
}

/// Load configuration from a list of database URLs.
pub fn from_urls(urls: &[String]) -> Result<ConfigAndUsers, Error> {
    let config = ConfigAndUsers::from_urls(urls)?;
    CONFIG.store(Arc::new(config.clone()));
    Ok(config)
}

/// Override some settings.
pub fn overrides(overrides: Overrides) {
    let mut config = (*config()).clone();
    let Overrides {
        default_pool_size,
        min_pool_size,
        session_mode,
    } = overrides;

    if let Some(default_pool_size) = default_pool_size {
        config.config.general.default_pool_size = default_pool_size;
    }

    if let Some(min_pool_size) = min_pool_size {
        config.config.general.min_pool_size = min_pool_size;
    }

    if let Some(true) = session_mode {
        config.config.general.pooler_mode = PoolerMode::Session;
    }

    CONFIG.store(Arc::new(config));
}

/// pgdog.toml and users.toml.
#[derive(Debug, Clone, Default)]
pub struct ConfigAndUsers {
    /// pgdog.toml
    pub config: Config,
    /// users.toml
    pub users: Users,
    /// Path to pgdog.toml.
    pub config_path: PathBuf,
    /// Path to users.toml.
    pub users_path: PathBuf,
}

impl ConfigAndUsers {
    /// Load configuration from disk or use defaults.
    pub fn load(config_path: &PathBuf, users_path: &PathBuf) -> Result<Self, Error> {
        let config: Config = if let Ok(config) = read_to_string(config_path) {
            let config = match toml::from_str(&config) {
                Ok(config) => config,
                Err(err) => return Err(Error::config(&config, err)),
            };
            info!("loaded \"{}\"", config_path.display());
            config
        } else {
            warn!(
                "\"{}\" doesn't exist, loading defaults instead",
                config_path.display()
            );
            Config::default()
        };

        if config.admin.random() {
            #[cfg(debug_assertions)]
            info!("[debug only] admin password: {}", config.admin.password);
            #[cfg(not(debug_assertions))]
            warn!("admin password has been randomly generated");
        }

        if config.multi_tenant.is_some() {
            info!("multi-tenant protection enabled");
        }

        let users: Users = if let Ok(users) = read_to_string(users_path) {
            let mut users: Users = toml::from_str(&users)?;
            users.check(&config);
            info!("loaded \"{}\"", users_path.display());
            users
        } else {
            warn!(
                "\"{}\" doesn't exist, loading defaults instead",
                users_path.display()
            );
            Users::default()
        };

        Ok(ConfigAndUsers {
            config,
            users,
            config_path: config_path.to_owned(),
            users_path: users_path.to_owned(),
        })
    }

    /// Prepared statements are enabled.
    pub fn prepared_statements(&self) -> bool {
        self.config.general.prepared_statements.enabled()
    }
}

/// Configuration.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Config {
    /// General configuration.
    #[serde(default)]
    pub general: General,
    /// Statistics.
    #[serde(default)]
    pub stats: Stats,
    /// TCP settings
    #[serde(default)]
    pub tcp: Tcp,
    /// Multi-tenant
    pub multi_tenant: Option<MultiTenant>,
    /// Servers.
    #[serde(default)]
    pub databases: Vec<Database>,
    #[serde(default)]
    pub plugins: Vec<Plugin>,
    #[serde(default)]
    pub admin: Admin,
    #[serde(default)]
    pub sharded_tables: Vec<ShardedTable>,
    #[serde(default)]
    pub manual_queries: Vec<ManualQuery>,
    #[serde(default)]
    pub omnisharded_tables: Vec<OmnishardedTables>,
}

impl Config {
    /// Organize all databases by name for quicker retrieval.
    pub fn databases(&self) -> HashMap<String, Vec<Vec<Database>>> {
        let mut databases = HashMap::new();
        for database in &self.databases {
            let entry = databases
                .entry(database.name.clone())
                .or_insert_with(Vec::new);
            while entry.len() <= database.shard {
                entry.push(vec![]);
            }
            entry
                .get_mut(database.shard)
                .unwrap()
                .push(database.clone());
        }
        databases
    }

    /// Organize sharded tables by database name.
    pub fn sharded_tables(&self) -> HashMap<String, Vec<ShardedTable>> {
        let mut tables = HashMap::new();

        for table in &self.sharded_tables {
            let entry = tables
                .entry(table.database.clone())
                .or_insert_with(Vec::new);
            entry.push(table.clone());
        }

        tables
    }

    pub fn omnisharded_tables(&self) -> HashMap<String, Vec<String>> {
        let mut tables = HashMap::new();

        for table in &self.omnisharded_tables {
            let entry = tables
                .entry(table.database.clone())
                .or_insert_with(Vec::new);
            for t in &table.tables {
                entry.push(t.clone());
            }
        }

        tables
    }

    /// Manual queries.
    pub fn manual_queries(&self) -> HashMap<String, ManualQuery> {
        let mut queries = HashMap::new();

        for query in &self.manual_queries {
            queries.insert(query.fingerprint.clone(), query.clone());
        }

        queries
    }

    pub fn check(&self) {
        // Check databases.
        let mut duplicate_primaries = HashSet::new();
        for database in self.databases.clone() {
            let id = (
                database.name.clone(),
                database.role,
                database.shard,
                database.port,
            );
            let new = duplicate_primaries.insert(id);
            if !new {
                warn!(
                    "database \"{}\" (shard={}) has a duplicate {}",
                    database.name, database.shard, database.role,
                );
            }
        }
    }

    /// Multi-tenanncy is enabled.
    pub fn multi_tenant(&self) -> &Option<MultiTenant> {
        &self.multi_tenant
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct General {
    /// Run on this address.
    #[serde(default = "General::host")]
    pub host: String,
    /// Run on this port.
    #[serde(default = "General::port")]
    pub port: u16,
    /// Spawn this many Tokio threads.
    #[serde(default = "General::workers")]
    pub workers: usize,
    /// Default pool size, e.g. 10.
    #[serde(default = "General::default_pool_size")]
    pub default_pool_size: usize,
    /// Minimum number of connections to maintain in the pool.
    #[serde(default = "General::min_pool_size")]
    pub min_pool_size: usize,
    /// Pooler mode, e.g. transaction.
    #[serde(default)]
    pub pooler_mode: PoolerMode,
    /// How often to check a connection.
    #[serde(default = "General::healthcheck_interval")]
    pub healthcheck_interval: u64,
    /// How often to issue a healthcheck via an idle connection.
    #[serde(default = "General::idle_healthcheck_interval")]
    pub idle_healthcheck_interval: u64,
    /// Delay idle healthchecks by this time at startup.
    #[serde(default = "General::idle_healthcheck_delay")]
    pub idle_healthcheck_delay: u64,
    /// Maximum duration of a ban.
    #[serde(default = "General::ban_timeout")]
    pub ban_timeout: u64,
    /// Rollback timeout.
    #[serde(default = "General::rollback_timeout")]
    pub rollback_timeout: u64,
    /// Load balancing strategy.
    #[serde(default = "General::load_balancing_strategy")]
    pub load_balancing_strategy: LoadBalancingStrategy,
    #[serde(default)]
    pub read_write_strategy: ReadWriteStrategy,
    /// TLS certificate.
    pub tls_certificate: Option<PathBuf>,
    /// TLS private key.
    pub tls_private_key: Option<PathBuf>,
    /// Shutdown timeout.
    #[serde(default = "General::default_shutdown_timeout")]
    pub shutdown_timeout: u64,
    /// Broadcast IP.
    pub broadcast_address: Option<Ipv4Addr>,
    /// Broadcast port.
    #[serde(default = "General::broadcast_port")]
    pub broadcast_port: u16,
    /// Load queries to file (warning: slow, don't use in production).
    #[serde(default)]
    pub query_log: Option<PathBuf>,
    /// Enable OpenMetrics server on this port.
    pub openmetrics_port: Option<u16>,
    /// Prepared statatements support.
    #[serde(default)]
    pub prepared_statements: PreparedStatements,
    /// Automatically add connection pools for user/database pairs we don't have.
    #[serde(default)]
    pub passthrough_auth: PassthoughAuth,
    /// Server connect timeout.
    #[serde(default = "General::default_connect_timeout")]
    pub connect_timeout: u64,
    #[serde(default = "General::default_query_timeout")]
    pub query_timeout: u64,
    /// Checkout timeout.
    #[serde(default = "General::checkout_timeout")]
    pub checkout_timeout: u64,
    /// Dry run for sharding. Parse the query, route to shard 0.
    #[serde(default)]
    pub dry_run: bool,
    /// Idle timeout.
    #[serde(default = "General::idle_timeout")]
    pub idle_timeout: u64,
    /// Mirror queue size.
    #[serde(default = "General::mirror_queue")]
    pub mirror_queue: usize,
    #[serde(default)]
    pub auth_type: AuthType,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum PreparedStatements {
    Disabled,
    #[default]
    Extended,
    Full,
}

impl PreparedStatements {
    pub fn full(&self) -> bool {
        matches!(self, PreparedStatements::Full)
    }

    pub fn enabled(&self) -> bool {
        !matches!(self, PreparedStatements::Disabled)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PassthoughAuth {
    #[default]
    Disabled,
    Enabled,
    EnabledPlain,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AuthType {
    Md5,
    #[default]
    Scram,
    Trust,
}

impl AuthType {
    pub fn md5(&self) -> bool {
        matches!(self, Self::Md5)
    }

    pub fn scram(&self) -> bool {
        matches!(self, Self::Scram)
    }

    pub fn trust(&self) -> bool {
        matches!(self, Self::Trust)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Copy)]
#[serde(rename_all = "snake_case")]
pub enum ReadWriteStrategy {
    #[default]
    Conservative,
    Aggressive,
}

impl Default for General {
    fn default() -> Self {
        Self {
            host: Self::host(),
            port: Self::port(),
            workers: Self::workers(),
            default_pool_size: Self::default_pool_size(),
            min_pool_size: Self::min_pool_size(),
            pooler_mode: PoolerMode::default(),
            healthcheck_interval: Self::healthcheck_interval(),
            idle_healthcheck_interval: Self::idle_healthcheck_interval(),
            idle_healthcheck_delay: Self::idle_healthcheck_delay(),
            ban_timeout: Self::ban_timeout(),
            rollback_timeout: Self::rollback_timeout(),
            load_balancing_strategy: Self::load_balancing_strategy(),
            read_write_strategy: ReadWriteStrategy::default(),
            tls_certificate: None,
            tls_private_key: None,
            shutdown_timeout: Self::default_shutdown_timeout(),
            broadcast_address: None,
            broadcast_port: Self::broadcast_port(),
            query_log: None,
            openmetrics_port: None,
            prepared_statements: PreparedStatements::default(),
            passthrough_auth: PassthoughAuth::default(),
            connect_timeout: Self::default_connect_timeout(),
            query_timeout: Self::default_query_timeout(),
            checkout_timeout: Self::checkout_timeout(),
            dry_run: bool::default(),
            idle_timeout: Self::idle_timeout(),
            mirror_queue: Self::mirror_queue(),
            auth_type: AuthType::default(),
        }
    }
}

impl General {
    fn host() -> String {
        "0.0.0.0".into()
    }

    fn port() -> u16 {
        6432
    }

    fn workers() -> usize {
        2
    }

    fn default_pool_size() -> usize {
        10
    }

    fn min_pool_size() -> usize {
        1
    }

    fn healthcheck_interval() -> u64 {
        30_000
    }

    fn idle_healthcheck_interval() -> u64 {
        30_000
    }

    fn idle_healthcheck_delay() -> u64 {
        5_000
    }

    fn ban_timeout() -> u64 {
        Duration::from_secs(300).as_millis() as u64
    }

    fn rollback_timeout() -> u64 {
        5_000
    }

    fn idle_timeout() -> u64 {
        Duration::from_secs(60).as_millis() as u64
    }

    fn default_query_timeout() -> u64 {
        Duration::MAX.as_millis() as u64
    }

    pub(crate) fn query_timeout(&self) -> Duration {
        Duration::from_millis(self.query_timeout)
    }

    fn load_balancing_strategy() -> LoadBalancingStrategy {
        LoadBalancingStrategy::Random
    }

    fn default_shutdown_timeout() -> u64 {
        60_000
    }

    fn default_connect_timeout() -> u64 {
        5_000
    }

    fn broadcast_port() -> u16 {
        Self::port() + 1
    }

    fn checkout_timeout() -> u64 {
        Duration::from_secs(5).as_millis() as u64
    }

    fn mirror_queue() -> usize {
        128
    }

    /// Get shutdown timeout as a duration.
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_millis(self.shutdown_timeout)
    }

    /// Get TLS config, if any.
    pub fn tls(&self) -> Option<(&PathBuf, &PathBuf)> {
        if let Some(cert) = &self.tls_certificate {
            if let Some(key) = &self.tls_private_key {
                return Some((cert, key));
            }
        }

        None
    }

    pub fn passthrough_auth(&self) -> bool {
        self.tls().is_some() && self.passthrough_auth == PassthoughAuth::Enabled
            || self.passthrough_auth == PassthoughAuth::EnabledPlain
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Stats {}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Copy, Eq, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum PoolerMode {
    #[default]
    Transaction,
    Session,
}

impl std::fmt::Display for PoolerMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transaction => write!(f, "transaction"),
            Self::Session => write!(f, "session"),
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
    /// Mirror of another database.
    pub mirror_of: Option<String>,
    /// Read-only mode.
    pub read_only: Option<bool>,
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

/// pgDog plugin.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Plugin {
    /// Plugin name.
    pub name: String,
}

/// Users and passwords.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Users {
    /// Users and passwords.
    #[serde(default)]
    pub users: Vec<User>,
}

impl Users {
    /// Organize users by database name.
    pub fn users(&self) -> HashMap<String, Vec<User>> {
        let mut users = HashMap::new();

        for user in &self.users {
            let entry = users.entry(user.database.clone()).or_insert_with(Vec::new);
            entry.push(user.clone());
        }

        users
    }

    pub fn check(&mut self, config: &Config) {
        for user in &mut self.users {
            if user.password().is_empty() {
                if !config.general.passthrough_auth() {
                    warn!(
                        "user \"{}\" doesn't have a password and passthrough auth is disabled",
                        user.name
                    );
                }

                if let Some(min_pool_size) = user.min_pool_size {
                    if min_pool_size > 0 {
                        warn!("user \"{}\" (database \"{}\") doesn't have a password configured, \
                            so we can't connect to the server to maintain min_pool_size of {}; setting it to 0", user.name, user.database, min_pool_size);
                        user.min_pool_size = Some(0);
                    }
                }
            }
        }
    }
}

/// User allowed to connect to pgDog.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, Ord, PartialOrd)]
#[serde(deny_unknown_fields)]
pub struct User {
    /// User name.
    pub name: String,
    /// Database name, from pgdog.toml.
    pub database: String,
    /// User's password.
    pub password: Option<String>,
    /// Pool size for this user pool, overriding `default_pool_size`.
    pub pool_size: Option<usize>,
    /// Minimum pool size for this user pool, overriding `min_pool_size`.
    pub min_pool_size: Option<usize>,
    /// Pooler mode.
    pub pooler_mode: Option<PoolerMode>,
    /// Server username.
    pub server_user: Option<String>,
    /// Server password.
    pub server_password: Option<String>,
    /// Statement timeout.
    pub statement_timeout: Option<u64>,
    /// Relication mode.
    #[serde(default)]
    pub replication_mode: bool,
    /// Sharding into this database.
    pub replication_sharding: Option<String>,
    /// Idle timeout.
    pub idle_timeout: Option<u64>,
    /// Read-only mode.
    pub read_only: Option<bool>,
}

impl User {
    pub fn password(&self) -> &str {
        if let Some(ref s) = self.password {
            s.as_str()
        } else {
            ""
        }
    }
}

/// Admin database settings.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Admin {
    /// Admin database name.
    #[serde(default = "Admin::name")]
    pub name: String,
    /// Admin user name.
    #[serde(default = "Admin::user")]
    pub user: String,
    /// Admin user's password.
    #[serde(default = "Admin::password")]
    pub password: String,
}

impl Default for Admin {
    fn default() -> Self {
        Self {
            name: Self::name(),
            user: Self::user(),
            password: admin_password(),
        }
    }
}

impl Admin {
    fn name() -> String {
        "admin".into()
    }

    fn user() -> String {
        "admin".into()
    }

    fn password() -> String {
        admin_password()
    }

    /// The password has been randomly generated.
    pub fn random(&self) -> bool {
        let prefix = "_pgdog_";
        self.password.starts_with(prefix) && self.password.len() == prefix.len() + 12
    }
}

fn admin_password() -> String {
    let pw = random_string(12);
    format!("_pgdog_{}", pw)
}

/// Sharded table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ShardedTable {
    /// Database this table belongs to.
    pub database: String,
    /// Table name. If none specified, all tables with the specified
    /// column are considered sharded.
    pub name: Option<String>,
    /// Table sharded on this column.
    #[serde(default)]
    pub column: String,
    /// This table is the primary sharding anchor (e.g. "users").
    #[serde(default)]
    pub primary: bool,
    /// Centroids for vector sharding.
    #[serde(default)]
    pub centroids: Vec<Vector>,
    #[serde(default)]
    pub centroids_path: Option<PathBuf>,
    /// Data type of the column.
    #[serde(default)]
    pub data_type: DataType,
    /// How many centroids to probe.
    #[serde(default)]
    pub centroid_probes: usize,
}

impl ShardedTable {
    /// Load centroids from file, if provided.
    ///
    /// Centroids can be very large vectors (1000+ columns).
    /// Hardcoding them in pgdog.toml is then impractical.
    pub fn load_centroids(&mut self) -> Result<(), Error> {
        if let Some(centroids_path) = &self.centroids_path {
            if let Ok(f) = std::fs::read_to_string(centroids_path) {
                let centroids: Vec<Vector> = serde_json::from_str(&f)?;
                self.centroids = centroids;
                info!("loaded {} centroids", self.centroids.len());
            } else {
                warn!(
                    "centroids at path \"{}\" not found",
                    centroids_path.display()
                );
            }
        }

        if self.centroid_probes < 1 {
            self.centroid_probes = (self.centroids.len() as f32).sqrt().ceil() as usize;
            if self.centroid_probes > 0 {
                info!("setting centroid probes to {}", self.centroid_probes);
            }
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default, Copy)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    #[default]
    Bigint,
    Uuid,
    Vector,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct OmnishardedTables {
    database: String,
    tables: Vec<String>,
}

/// Queries with manual routing rules.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ManualQuery {
    pub fingerprint: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub struct Tcp {
    #[serde(default = "Tcp::default_keepalive")]
    keepalive: bool,
    user_timeout: Option<u64>,
    time: Option<u64>,
    interval: Option<u64>,
    retries: Option<u32>,
}

impl std::fmt::Display for Tcp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "keepalive={} user_timeout={} time={} interval={}, retries={}",
            self.keepalive(),
            human_duration_optional(self.user_timeout()),
            human_duration_optional(self.time()),
            human_duration_optional(self.interval()),
            if let Some(retries) = self.retries() {
                retries.to_string()
            } else {
                "default".into()
            }
        )
    }
}

impl Default for Tcp {
    fn default() -> Self {
        Self {
            keepalive: Self::default_keepalive(),
            user_timeout: None,
            time: None,
            interval: None,
            retries: None,
        }
    }
}

impl Tcp {
    fn default_keepalive() -> bool {
        true
    }

    pub fn keepalive(&self) -> bool {
        self.keepalive
    }

    pub fn time(&self) -> Option<Duration> {
        self.time.map(Duration::from_millis)
    }

    pub fn interval(&self) -> Option<Duration> {
        self.interval.map(Duration::from_millis)
    }

    pub fn user_timeout(&self) -> Option<Duration> {
        self.user_timeout.map(Duration::from_millis)
    }

    pub fn retries(&self) -> Option<u32> {
        self.retries
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct MultiTenant {
    pub column: String,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_basic() {
        let source = r#"
[general]
host = "0.0.0.0"
port = 6432
default_pool_size = 15
pooler_mode = "transaction"

[[databases]]
name = "production"
role = "primary"
host = "127.0.0.1"
port = 5432
database_name = "postgres"

[tcp]
keepalive = true
interval = 5000
time = 1000
user_timeout = 1000
retries = 5

[[plugins]]
name = "pgdog_routing"

[multi_tenant]
column = "tenant_id"
"#;

        let config: Config = toml::from_str(source).unwrap();
        assert_eq!(config.databases[0].name, "production");
        assert_eq!(config.plugins[0].name, "pgdog_routing");
        assert!(config.tcp.keepalive());
        assert_eq!(config.tcp.interval().unwrap(), Duration::from_millis(5000));
        assert_eq!(
            config.tcp.user_timeout().unwrap(),
            Duration::from_millis(1000)
        );
        assert_eq!(config.tcp.time().unwrap(), Duration::from_millis(1000));
        assert_eq!(config.tcp.retries().unwrap(), 5);
        assert_eq!(config.multi_tenant.unwrap().column, "tenant_id");
    }
}
