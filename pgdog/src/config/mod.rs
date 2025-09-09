//! Configuration.

pub mod convert;
pub mod error;
pub mod overrides;
pub mod url;

use error::Error;
pub use overrides::Overrides;
use parking_lot::Mutex;

use std::collections::HashSet;
use std::env;
use std::fs::read_to_string;
use std::hash::{Hash, Hasher as StdHasher};
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    path::PathBuf,
};

use crate::frontend::router::sharding::Mapping;
use crate::net::messages::Vector;
use crate::util::{human_duration_optional, random_string};
use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tracing::info;
use tracing::warn;

static CONFIG: Lazy<ArcSwap<ConfigAndUsers>> =
    Lazy::new(|| ArcSwap::from_pointee(ConfigAndUsers::default()));

static LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

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
    let _lock = LOCK.lock();
    let config = (*config()).clone();
    let config = config.databases_from_urls(urls)?;
    CONFIG.store(Arc::new(config.clone()));
    Ok(config)
}

/// Extract all database URLs from the environment and
/// create the config.
pub fn from_env() -> Result<ConfigAndUsers, Error> {
    let mut urls = vec![];
    let mut index = 1;
    while let Ok(url) = env::var(format!("PGDOG_DATABASE_URL_{}", index)) {
        urls.push(url);
        index += 1;
    }

    if urls.is_empty() {
        Err(Error::NoDbsInEnv)
    } else {
        from_urls(&urls)
    }
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
        // Disable prepared statements automatically in session mode
        if self.config.general.pooler_mode == PoolerMode::Session {
            false
        } else {
            self.config.general.prepared_statements.enabled()
        }
    }

    /// Prepared statements are in "full" mode (used for query parser decision).
    pub fn prepared_statements_full(&self) -> bool {
        self.config.general.prepared_statements.full()
    }

    pub fn pub_sub_enabled(&self) -> bool {
        self.config.general.pub_sub_channel_size > 0
    }
}

/// Configuration.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
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

    /// List of sharded tables.
    #[serde(default)]
    pub sharded_tables: Vec<ShardedTable>,

    /// Queries routed manually to a single shard.
    #[serde(default)]
    pub manual_queries: Vec<ManualQuery>,

    /// List of omnisharded tables.
    #[serde(default)]
    pub omnisharded_tables: Vec<OmnishardedTables>,

    /// Explicit sharding key mappings.
    #[serde(default)]
    pub sharded_mappings: Vec<ShardedMapping>,

    /// Replica lag configuration.
    #[serde(default, deserialize_with = "ReplicaLag::deserialize_optional")]
    pub replica_lag: Option<ReplicaLag>,

    /// Replication config.
    #[serde(default)]
    pub replication: Replication,

    /// Mirroring configurations.
    #[serde(default)]
    pub mirroring: Vec<Mirroring>,
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

    /// Sharded mappings.
    pub fn sharded_mappings(
        &self,
    ) -> HashMap<(String, String, Option<String>), Vec<ShardedMapping>> {
        let mut mappings = HashMap::new();

        for mapping in &self.sharded_mappings {
            let mapping = mapping.clone();
            let entry = mappings
                .entry((
                    mapping.database.clone(),
                    mapping.column.clone(),
                    mapping.table.clone(),
                ))
                .or_insert_with(Vec::new);
            entry.push(mapping);
        }

        mappings
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

    /// Get mirroring configuration for a specific source/destination pair.
    pub fn get_mirroring_config(
        &self,
        source_db: &str,
        destination_db: &str,
    ) -> Option<MirrorConfig> {
        self.mirroring
            .iter()
            .find(|m| m.source_db == source_db && m.destination_db == destination_db)
            .map(|m| MirrorConfig {
                queue_length: m.queue_length.unwrap_or(self.general.mirror_queue),
                exposure: m.exposure.unwrap_or(self.general.mirror_exposure),
            })
    }

    /// Get all mirroring configurations mapped by source database.
    pub fn mirroring_by_source(&self) -> HashMap<String, Vec<(String, MirrorConfig)>> {
        let mut result = HashMap::new();

        for mirror in &self.mirroring {
            let config = MirrorConfig {
                queue_length: mirror.queue_length.unwrap_or(self.general.mirror_queue),
                exposure: mirror.exposure.unwrap_or(self.general.mirror_exposure),
            };

            result
                .entry(mirror.source_db.clone())
                .or_insert_with(Vec::new)
                .push((mirror.destination_db.clone(), config));
        }

        result
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
    /// Healthcheck timeout.
    #[serde(default = "General::healthcheck_timeout")]
    pub healthcheck_timeout: u64,
    /// Maximum duration of a ban.
    #[serde(default = "General::ban_timeout")]
    pub ban_timeout: u64,
    /// Rollback timeout.
    #[serde(default = "General::rollback_timeout")]
    pub rollback_timeout: u64,
    /// Load balancing strategy.
    #[serde(default = "General::load_balancing_strategy")]
    pub load_balancing_strategy: LoadBalancingStrategy,
    /// How aggressive should the query parser be in determining reads.
    #[serde(default)]
    pub read_write_strategy: ReadWriteStrategy,
    /// Read write split.
    #[serde(default)]
    pub read_write_split: ReadWriteSplit,
    /// TLS certificate.
    pub tls_certificate: Option<PathBuf>,
    /// TLS private key.
    pub tls_private_key: Option<PathBuf>,
    /// TLS verification mode (for connecting to servers)
    #[serde(default = "General::default_tls_verify")]
    pub tls_verify: TlsVerifyMode,
    /// TLS CA certificate (for connecting to servers).
    pub tls_server_ca_certificate: Option<PathBuf>,
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
    /// OpenMetrics prefix.
    pub openmetrics_namespace: Option<String>,
    /// Prepared statatements support.
    #[serde(default)]
    prepared_statements: PreparedStatements,
    /// Limit on the number of prepared statements in the server cache.
    #[serde(default = "General::prepared_statements_limit")]
    pub prepared_statements_limit: usize,
    #[serde(default = "General::query_cache_limit")]
    pub query_cache_limit: usize,
    /// Automatically add connection pools for user/database pairs we don't have.
    #[serde(default = "General::default_passthrough_auth")]
    pub passthrough_auth: PassthoughAuth,
    /// Server connect timeout.
    #[serde(default = "General::default_connect_timeout")]
    pub connect_timeout: u64,
    /// Attempt connections multiple times on bad networks.
    #[serde(default = "General::connect_attempts")]
    pub connect_attempts: u64,
    /// How long to wait between connection attempts.
    #[serde(default = "General::default_connect_attempt_delay")]
    pub connect_attempt_delay: u64,
    /// How long to wait for a query to return the result before aborting. Dangerous: don't use unless your network is bad.
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
    /// Client idle timeout.
    #[serde(default = "General::default_client_idle_timeout")]
    pub client_idle_timeout: u64,
    /// Mirror queue size.
    #[serde(default = "General::mirror_queue")]
    pub mirror_queue: usize,
    /// Mirror exposure
    #[serde(default = "General::mirror_exposure")]
    pub mirror_exposure: f32,
    #[serde(default)]
    pub auth_type: AuthType,
    /// Disable cross-shard queries.
    #[serde(default)]
    pub cross_shard_disabled: bool,
    /// How often to refresh DNS entries, in ms.
    #[serde(default)]
    pub dns_ttl: Option<u64>,
    /// LISTEN/NOTIFY channel size.
    #[serde(default)]
    pub pub_sub_channel_size: usize,
    /// Log client connections.
    #[serde(default = "General::log_connections")]
    pub log_connections: bool,
    /// Log client disconnections.
    #[serde(default = "General::log_disconnections")]
    pub log_disconnections: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
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

impl FromStr for PreparedStatements {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disabled" => Ok(Self::Disabled),
            "extended" => Ok(Self::Extended),
            "full" => Ok(Self::Full),
            _ => Err(format!("Invalid prepared statements mode: {}", s)),
        }
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

impl FromStr for AuthType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "md5" => Ok(Self::Md5),
            "scram" => Ok(Self::Scram),
            "trust" => Ok(Self::Trust),
            _ => Err(format!("Invalid auth type: {}", s)),
        }
    }
}

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

impl Default for General {
    fn default() -> Self {
        Self {
            host: Self::host(),
            port: Self::port(),
            workers: Self::workers(),
            default_pool_size: Self::default_pool_size(),
            min_pool_size: Self::min_pool_size(),
            pooler_mode: Self::pooler_mode(),
            healthcheck_interval: Self::healthcheck_interval(),
            idle_healthcheck_interval: Self::idle_healthcheck_interval(),
            idle_healthcheck_delay: Self::idle_healthcheck_delay(),
            healthcheck_timeout: Self::healthcheck_timeout(),
            ban_timeout: Self::ban_timeout(),
            rollback_timeout: Self::rollback_timeout(),
            load_balancing_strategy: Self::load_balancing_strategy(),
            read_write_strategy: Self::read_write_strategy(),
            read_write_split: Self::read_write_split(),
            tls_certificate: Self::tls_certificate(),
            tls_private_key: Self::tls_private_key(),
            tls_verify: Self::default_tls_verify(),
            tls_server_ca_certificate: Self::tls_server_ca_certificate(),
            shutdown_timeout: Self::default_shutdown_timeout(),
            broadcast_address: Self::broadcast_address(),
            broadcast_port: Self::broadcast_port(),
            query_log: Self::query_log(),
            openmetrics_port: Self::openmetrics_port(),
            openmetrics_namespace: Self::openmetrics_namespace(),
            prepared_statements: Self::prepared_statements(),
            prepared_statements_limit: Self::prepared_statements_limit(),
            query_cache_limit: Self::query_cache_limit(),
            passthrough_auth: Self::default_passthrough_auth(),
            connect_timeout: Self::default_connect_timeout(),
            connect_attempt_delay: Self::default_connect_attempt_delay(),
            connect_attempts: Self::connect_attempts(),
            query_timeout: Self::default_query_timeout(),
            checkout_timeout: Self::checkout_timeout(),
            dry_run: Self::dry_run(),
            idle_timeout: Self::idle_timeout(),
            client_idle_timeout: Self::default_client_idle_timeout(),
            mirror_queue: Self::mirror_queue(),
            mirror_exposure: Self::mirror_exposure(),
            auth_type: Self::auth_type(),
            cross_shard_disabled: Self::cross_shard_disabled(),
            dns_ttl: Self::default_dns_ttl(),
            pub_sub_channel_size: Self::pub_sub_channel_size(),
            log_connections: Self::log_connections(),
            log_disconnections: Self::log_disconnections(),
        }
    }
}

impl General {
    fn env_or_default<T: std::str::FromStr>(env_var: &str, default: T) -> T {
        env::var(env_var)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    fn env_string_or_default(env_var: &str, default: &str) -> String {
        env::var(env_var).unwrap_or_else(|_| default.to_string())
    }

    fn env_bool_or_default(env_var: &str, default: bool) -> bool {
        env::var(env_var)
            .ok()
            .and_then(|v| match v.to_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => Some(true),
                "false" | "0" | "no" | "off" => Some(false),
                _ => None,
            })
            .unwrap_or(default)
    }

    fn env_option<T: std::str::FromStr>(env_var: &str) -> Option<T> {
        env::var(env_var).ok().and_then(|v| v.parse().ok())
    }

    fn env_option_string(env_var: &str) -> Option<String> {
        env::var(env_var).ok().filter(|s| !s.is_empty())
    }

    fn env_enum_or_default<T: std::str::FromStr + Default>(env_var: &str) -> T {
        env::var(env_var)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_default()
    }

    fn host() -> String {
        Self::env_string_or_default("PGDOG_HOST", "0.0.0.0")
    }

    fn port() -> u16 {
        Self::env_or_default("PGDOG_PORT", 6432)
    }

    fn workers() -> usize {
        Self::env_or_default("PGDOG_WORKERS", 2)
    }

    fn default_pool_size() -> usize {
        Self::env_or_default("PGDOG_DEFAULT_POOL_SIZE", 10)
    }

    fn min_pool_size() -> usize {
        Self::env_or_default("PGDOG_MIN_POOL_SIZE", 1)
    }

    fn healthcheck_interval() -> u64 {
        Self::env_or_default("PGDOG_HEALTHCHECK_INTERVAL", 30_000)
    }

    fn idle_healthcheck_interval() -> u64 {
        Self::env_or_default("PGDOG_IDLE_HEALTHCHECK_INTERVAL", 30_000)
    }

    fn idle_healthcheck_delay() -> u64 {
        Self::env_or_default("PGDOG_IDLE_HEALTHCHECK_DELAY", 5_000)
    }

    fn ban_timeout() -> u64 {
        Self::env_or_default(
            "PGDOG_BAN_TIMEOUT",
            Duration::from_secs(300).as_millis() as u64,
        )
    }

    fn rollback_timeout() -> u64 {
        Self::env_or_default("PGDOG_ROLLBACK_TIMEOUT", 5_000)
    }

    fn idle_timeout() -> u64 {
        Self::env_or_default(
            "PGDOG_IDLE_TIMEOUT",
            Duration::from_secs(60).as_millis() as u64,
        )
    }

    fn default_client_idle_timeout() -> u64 {
        Self::env_or_default(
            "PGDOG_CLIENT_IDLE_TIMEOUT",
            Duration::MAX.as_millis() as u64,
        )
    }

    fn default_query_timeout() -> u64 {
        Self::env_or_default("PGDOG_QUERY_TIMEOUT", Duration::MAX.as_millis() as u64)
    }

    pub(crate) fn query_timeout(&self) -> Duration {
        Duration::from_millis(self.query_timeout)
    }

    pub fn dns_ttl(&self) -> Option<Duration> {
        self.dns_ttl.map(Duration::from_millis)
    }

    pub(crate) fn client_idle_timeout(&self) -> Duration {
        Duration::from_millis(self.client_idle_timeout)
    }

    pub(crate) fn connect_attempt_delay(&self) -> Duration {
        Duration::from_millis(self.connect_attempt_delay)
    }

    fn load_balancing_strategy() -> LoadBalancingStrategy {
        Self::env_enum_or_default("PGDOG_LOAD_BALANCING_STRATEGY")
    }

    fn default_tls_verify() -> TlsVerifyMode {
        env::var("PGDOG_TLS_VERIFY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(TlsVerifyMode::Prefer)
    }

    fn default_shutdown_timeout() -> u64 {
        Self::env_or_default("PGDOG_SHUTDOWN_TIMEOUT", 60_000)
    }

    fn default_connect_timeout() -> u64 {
        Self::env_or_default("PGDOG_CONNECT_TIMEOUT", 5_000)
    }

    fn default_connect_attempt_delay() -> u64 {
        Self::env_or_default("PGDOG_CONNECT_ATTEMPT_DELAY", 0)
    }

    fn connect_attempts() -> u64 {
        Self::env_or_default("PGDOG_CONNECT_ATTEMPTS", 1)
    }

    fn pooler_mode() -> PoolerMode {
        Self::env_enum_or_default("PGDOG_POOLER_MODE")
    }

    fn read_write_strategy() -> ReadWriteStrategy {
        Self::env_enum_or_default("PGDOG_READ_WRITE_STRATEGY")
    }

    fn read_write_split() -> ReadWriteSplit {
        Self::env_enum_or_default("PGDOG_READ_WRITE_SPLIT")
    }

    fn prepared_statements() -> PreparedStatements {
        Self::env_enum_or_default("PGDOG_PREPARED_STATEMENTS")
    }

    fn auth_type() -> AuthType {
        Self::env_enum_or_default("PGDOG_AUTH_TYPE")
    }

    fn tls_certificate() -> Option<PathBuf> {
        Self::env_option_string("PGDOG_TLS_CERTIFICATE").map(PathBuf::from)
    }

    fn tls_private_key() -> Option<PathBuf> {
        Self::env_option_string("PGDOG_TLS_PRIVATE_KEY").map(PathBuf::from)
    }

    fn tls_server_ca_certificate() -> Option<PathBuf> {
        Self::env_option_string("PGDOG_TLS_SERVER_CA_CERTIFICATE").map(PathBuf::from)
    }

    fn query_log() -> Option<PathBuf> {
        Self::env_option_string("PGDOG_QUERY_LOG").map(PathBuf::from)
    }

    fn openmetrics_port() -> Option<u16> {
        Self::env_option("PGDOG_OPENMETRICS_PORT")
    }

    fn openmetrics_namespace() -> Option<String> {
        Self::env_option_string("PGDOG_OPENMETRICS_NAMESPACE")
    }

    fn default_dns_ttl() -> Option<u64> {
        Self::env_option("PGDOG_DNS_TTL")
    }

    fn pub_sub_channel_size() -> usize {
        Self::env_or_default("PGDOG_PUB_SUB_CHANNEL_SIZE", 0)
    }

    fn dry_run() -> bool {
        Self::env_bool_or_default("PGDOG_DRY_RUN", false)
    }

    fn cross_shard_disabled() -> bool {
        Self::env_bool_or_default("PGDOG_CROSS_SHARD_DISABLED", false)
    }

    fn broadcast_address() -> Option<Ipv4Addr> {
        Self::env_option("PGDOG_BROADCAST_ADDRESS")
    }

    fn broadcast_port() -> u16 {
        Self::env_or_default("PGDOG_BROADCAST_PORT", Self::port() + 1)
    }

    fn healthcheck_timeout() -> u64 {
        Self::env_or_default(
            "PGDOG_HEALTHCHECK_TIMEOUT",
            Duration::from_secs(5).as_millis() as u64,
        )
    }

    fn checkout_timeout() -> u64 {
        Self::env_or_default(
            "PGDOG_CHECKOUT_TIMEOUT",
            Duration::from_secs(5).as_millis() as u64,
        )
    }

    fn mirror_queue() -> usize {
        Self::env_or_default("PGDOG_MIRROR_QUEUE", 128)
    }

    fn mirror_exposure() -> f32 {
        Self::env_or_default("PGDOG_MIRROR_EXPOSURE", 1.0)
    }

    fn prepared_statements_limit() -> usize {
        Self::env_or_default("PGDOG_PREPARED_STATEMENTS_LIMIT", usize::MAX)
    }

    fn query_cache_limit() -> usize {
        Self::env_or_default("PGDOG_QUERY_CACHE_LIMIT", usize::MAX)
    }

    fn log_connections() -> bool {
        Self::env_bool_or_default("PGDOG_LOG_CONNECTIONS", true)
    }

    fn log_disconnections() -> bool {
        Self::env_bool_or_default("PGDOG_LOG_DISCONNECTIONS", true)
    }

    fn default_passthrough_auth() -> PassthoughAuth {
        if let Ok(auth) = env::var("PGDOG_PASSTHROUGH_AUTH") {
            // TODO: figure out why toml::from_str doesn't work.
            match auth.as_str() {
                "enabled" => PassthoughAuth::Enabled,
                "disabled" => PassthoughAuth::Disabled,
                "enabled_plain" => PassthoughAuth::EnabledPlain,
                _ => PassthoughAuth::default(),
            }
        } else {
            PassthoughAuth::default()
        }
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

    /// Support for LISTEN/NOTIFY.
    pub fn pub_sub_enabled(&self) -> bool {
        self.pub_sub_channel_size > 0
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
        match s.to_lowercase().replace('_', "").replace('-', "").as_str() {
            "random" => Ok(Self::Random),
            "roundrobin" => Ok(Self::RoundRobin),
            "leastactiveconnections" => Ok(Self::LeastActiveConnections),
            _ => Err(format!("Invalid load balancing strategy: {}", s)),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Copy)]
#[serde(rename_all = "snake_case")]
pub enum TlsVerifyMode {
    #[default]
    Disabled,
    Prefer,
    VerifyCa,
    VerifyFull,
}

impl FromStr for TlsVerifyMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace('_', "").replace('-', "").as_str() {
            "disabled" => Ok(Self::Disabled),
            "prefer" => Ok(Self::Prefer),
            "verifyca" => Ok(Self::VerifyCa),
            "verifyfull" => Ok(Self::VerifyFull),
            _ => Err(format!("Invalid TLS verify mode: {}", s)),
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
        match s.to_lowercase().replace('_', "").replace('-', "").as_str() {
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
#[serde(deny_unknown_fields)]
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
    /// Schema owner.
    #[serde(default)]
    pub schema_admin: bool,
    /// Disable cross-shard queries for this user.
    pub cross_shard_disabled: Option<bool>,
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
    if let Ok(password) = env::var("PGDOG_ADMIN_PASSWORD") {
        password
    } else {
        let pw = random_string(12);
        format!("_pgdog_{}", pw)
    }
}

/// Sharded table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
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
    /// Hasher function.
    #[serde(default)]
    pub hasher: Hasher,
    /// Explicit routing rules.
    #[serde(skip, default)]
    pub mapping: Option<Mapping>,
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

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Hasher {
    #[default]
    Postgres,
    Sha1,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default, Copy, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    #[default]
    Bigint,
    Uuid,
    Vector,
    Varchar,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ShardedMapping {
    pub database: String,
    pub column: String,
    pub table: Option<String>,
    pub kind: ShardedMappingKind,
    pub start: Option<FlexibleType>,
    pub end: Option<FlexibleType>,
    #[serde(default)]
    pub values: HashSet<FlexibleType>,
    pub shard: usize,
}

impl Hash for ShardedMapping {
    fn hash<H: StdHasher>(&self, state: &mut H) {
        self.database.hash(state);
        self.column.hash(state);
        self.table.hash(state);
        self.kind.hash(state);
        self.start.hash(state);
        self.end.hash(state);

        // Hash the values in a deterministic way by XORing their individual hashes
        let mut values_hash = 0u64;
        for value in &self.values {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            values_hash ^= hasher.finish();
        }
        values_hash.hash(state);

        self.shard.hash(state);
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default, Hash, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ShardedMappingKind {
    #[default]
    List,
    Range,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Eq, Hash)]
#[serde(untagged)]
pub enum FlexibleType {
    Integer(i64),
    Uuid(uuid::Uuid),
    String(String),
}

impl From<i64> for FlexibleType {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<uuid::Uuid> for FlexibleType {
    fn from(value: uuid::Uuid) -> Self {
        Self::Uuid(value)
    }
}

impl From<String> for FlexibleType {
    fn from(value: String) -> Self {
        Self::String(value)
    }
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

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Tcp {
    #[serde(default = "Tcp::default_keepalive")]
    keepalive: bool,
    user_timeout: Option<u64>,
    time: Option<u64>,
    interval: Option<u64>,
    retries: Option<u32>,
    congestion_control: Option<String>,
}

impl std::fmt::Display for Tcp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "keepalive={} user_timeout={} time={} interval={}, retries={}, congestion_control={}",
            self.keepalive(),
            human_duration_optional(self.user_timeout()),
            human_duration_optional(self.time()),
            human_duration_optional(self.interval()),
            if let Some(retries) = self.retries() {
                retries.to_string()
            } else {
                "default".into()
            },
            if let Some(ref c) = self.congestion_control {
                c.as_str()
            } else {
                ""
            },
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
            congestion_control: None,
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

    pub fn congestion_control(&self) -> &Option<String> {
        &self.congestion_control
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct MultiTenant {
    pub column: String,
}

//--------------------------------------------------------------------------------------------------
//----- Replica Lag --------------------------------------------------------------------------------

#[derive(Deserialize)]
struct RawReplicaLag {
    #[serde(default)]
    check_interval: Option<u64>,
    #[serde(default)]
    max_age: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ReplicaLag {
    pub check_interval: Duration,
    pub max_age: Duration,
}

impl ReplicaLag {
    fn default_max_age() -> Duration {
        Duration::from_millis(25)
    }

    fn default_check_interval() -> Duration {
        Duration::from_millis(1000)
    }

    /// Custom “all-or-none” deserializer that returns Option<Self>.
    pub fn deserialize_optional<'de, D>(de: D) -> Result<Option<Self>, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let maybe: Option<RawReplicaLag> = Option::deserialize(de)?;

        Ok(match maybe {
            None => None,

            Some(RawReplicaLag {
                check_interval: None,
                max_age: None,
            }) => None,

            Some(RawReplicaLag {
                check_interval: Some(ci_u64),
                max_age: Some(ma_u64),
            }) => Some(ReplicaLag {
                check_interval: Duration::from_millis(ci_u64),
                max_age: Duration::from_millis(ma_u64),
            }),

            Some(RawReplicaLag {
                check_interval: None,
                max_age: Some(ma_u64),
            }) => Some(ReplicaLag {
                check_interval: Self::default_check_interval(),
                max_age: Duration::from_millis(ma_u64),
            }),

            _ => {
                return Err(serde::de::Error::custom(
                    "replica_lag: cannot set check_interval without max_age",
                ))
            }
        })
    }
}

// NOTE: serialize and deserialize are not inverses.
// - Normally you'd expect ser <-> deser to round-trip, but here deser applies defaults...
//   for missing fields
// - Serializes takes those applied defaults into account so that ReplicaLag always reflects...
//   the actual effective values.
// - This ensures pgdog.admin sees the true config that is applied, not just what was configured.

impl Serialize for ReplicaLag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ReplicaLag", 2)?;
        state.serialize_field("check_interval", &(self.check_interval.as_millis() as u64))?;
        state.serialize_field("max_age", &(self.max_age.as_millis() as u64))?;
        state.end()
    }
}

impl Default for ReplicaLag {
    fn default() -> Self {
        Self {
            check_interval: Self::default_check_interval(),
            max_age: Self::default_max_age(),
        }
    }
}

/// Replication configuration.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Replication {
    /// Path to the pg_dump executable.
    #[serde(default = "Replication::pg_dump_path")]
    pub pg_dump_path: PathBuf,
}

impl Replication {
    fn pg_dump_path() -> PathBuf {
        PathBuf::from("pg_dump")
    }
}

impl Default for Replication {
    fn default() -> Self {
        Self {
            pg_dump_path: Self::pg_dump_path(),
        }
    }
}

/// Mirroring configuration.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct Mirroring {
    /// Source database name to mirror from.
    pub source_db: String,
    /// Destination database name to mirror to.
    pub destination_db: String,
    /// Queue length for this mirror (overrides global mirror_queue).
    pub queue_length: Option<usize>,
    /// Exposure for this mirror (overrides global mirror_exposure).
    pub exposure: Option<f32>,
}

/// Runtime mirror configuration with resolved values.
#[derive(Debug, Clone)]
pub struct MirrorConfig {
    /// Queue length for this mirror.
    pub queue_length: usize,
    /// Exposure for this mirror.
    pub exposure: f32,
}

#[cfg(test)]
pub mod test {
    use crate::backend::databases::init;

    use super::*;

    pub fn load_test() {
        let mut config = ConfigAndUsers::default();
        config.config.databases = vec![Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 5432,
            ..Default::default()
        }];
        config.users.users = vec![User {
            name: "pgdog".into(),
            database: "pgdog".into(),
            password: Some("pgdog".into()),
            ..Default::default()
        }];

        set(config).unwrap();
        init();
    }

    pub fn load_test_replicas() {
        let mut config = ConfigAndUsers::default();
        config.config.databases = vec![
            Database {
                name: "pgdog".into(),
                host: "127.0.0.1".into(),
                port: 5432,
                role: Role::Primary,
                ..Default::default()
            },
            Database {
                name: "pgdog".into(),
                host: "127.0.0.1".into(),
                port: 5432,
                role: Role::Replica,
                read_only: Some(true),
                ..Default::default()
            },
        ];
        config.config.general.load_balancing_strategy = LoadBalancingStrategy::RoundRobin;
        config.users.users = vec![User {
            name: "pgdog".into(),
            database: "pgdog".into(),
            password: Some("pgdog".into()),
            ..Default::default()
        }];

        set(config).unwrap();
        init();
    }

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

    #[test]
    fn test_prepared_statements_disabled_in_session_mode() {
        let mut config = ConfigAndUsers::default();

        // Test transaction mode (default) - prepared statements should be enabled
        config.config.general.pooler_mode = PoolerMode::Transaction;
        config.config.general.prepared_statements = PreparedStatements::Extended;
        assert!(
            config.prepared_statements(),
            "Prepared statements should be enabled in transaction mode"
        );

        // Test session mode - prepared statements should be disabled
        config.config.general.pooler_mode = PoolerMode::Session;
        config.config.general.prepared_statements = PreparedStatements::Extended;
        assert!(
            !config.prepared_statements(),
            "Prepared statements should be disabled in session mode"
        );

        // Test session mode with full prepared statements - should still be disabled
        config.config.general.pooler_mode = PoolerMode::Session;
        config.config.general.prepared_statements = PreparedStatements::Full;
        assert!(
            !config.prepared_statements(),
            "Prepared statements should be disabled in session mode even when set to Full"
        );

        // Test transaction mode with disabled prepared statements - should remain disabled
        config.config.general.pooler_mode = PoolerMode::Transaction;
        config.config.general.prepared_statements = PreparedStatements::Disabled;
        assert!(!config.prepared_statements(), "Prepared statements should remain disabled when explicitly set to Disabled in transaction mode");
    }

    #[test]
    fn test_mirroring_config() {
        let source = r#"
[general]
host = "0.0.0.0"
port = 6432
mirror_queue = 128
mirror_exposure = 1.0

[[databases]]
name = "source_db"
host = "127.0.0.1"
port = 5432

[[databases]]
name = "destination_db1"
host = "127.0.0.1"
port = 5433

[[databases]]
name = "destination_db2"
host = "127.0.0.1"
port = 5434

[[mirroring]]
source_db = "source_db"
destination_db = "destination_db1"
queue_length = 256
exposure = 0.5

[[mirroring]]
source_db = "source_db"
destination_db = "destination_db2"
exposure = 0.75
"#;

        let config: Config = toml::from_str(source).unwrap();

        // Verify we have 2 mirroring configurations
        assert_eq!(config.mirroring.len(), 2);

        // Check first mirroring config
        assert_eq!(config.mirroring[0].source_db, "source_db");
        assert_eq!(config.mirroring[0].destination_db, "destination_db1");
        assert_eq!(config.mirroring[0].queue_length, Some(256));
        assert_eq!(config.mirroring[0].exposure, Some(0.5));

        // Check second mirroring config
        assert_eq!(config.mirroring[1].source_db, "source_db");
        assert_eq!(config.mirroring[1].destination_db, "destination_db2");
        assert_eq!(config.mirroring[1].queue_length, None); // Should use global default
        assert_eq!(config.mirroring[1].exposure, Some(0.75));

        // Verify global defaults are still set
        assert_eq!(config.general.mirror_queue, 128);
        assert_eq!(config.general.mirror_exposure, 1.0);

        // Test get_mirroring_config method
        let mirror_config = config
            .get_mirroring_config("source_db", "destination_db1")
            .unwrap();
        assert_eq!(mirror_config.queue_length, 256);
        assert_eq!(mirror_config.exposure, 0.5);

        let mirror_config2 = config
            .get_mirroring_config("source_db", "destination_db2")
            .unwrap();
        assert_eq!(mirror_config2.queue_length, 128); // Uses global default
        assert_eq!(mirror_config2.exposure, 0.75);

        // Non-existent mirror config should return None
        assert!(config
            .get_mirroring_config("source_db", "non_existent")
            .is_none());
    }

    #[test]
    fn test_env_workers() {
        env::set_var("PGDOG_WORKERS", "8");
        assert_eq!(General::workers(), 8);
        env::remove_var("PGDOG_WORKERS");
        assert_eq!(General::workers(), 2);
    }

    #[test]
    fn test_env_pool_sizes() {
        env::set_var("PGDOG_DEFAULT_POOL_SIZE", "50");
        env::set_var("PGDOG_MIN_POOL_SIZE", "5");

        assert_eq!(General::default_pool_size(), 50);
        assert_eq!(General::min_pool_size(), 5);

        env::remove_var("PGDOG_DEFAULT_POOL_SIZE");
        env::remove_var("PGDOG_MIN_POOL_SIZE");

        assert_eq!(General::default_pool_size(), 10);
        assert_eq!(General::min_pool_size(), 1);
    }

    #[test]
    fn test_env_timeouts() {
        env::set_var("PGDOG_HEALTHCHECK_INTERVAL", "60000");
        env::set_var("PGDOG_HEALTHCHECK_TIMEOUT", "10000");
        env::set_var("PGDOG_CONNECT_TIMEOUT", "10000");
        env::set_var("PGDOG_CHECKOUT_TIMEOUT", "15000");
        env::set_var("PGDOG_IDLE_TIMEOUT", "120000");

        assert_eq!(General::healthcheck_interval(), 60000);
        assert_eq!(General::healthcheck_timeout(), 10000);
        assert_eq!(General::default_connect_timeout(), 10000);
        assert_eq!(General::checkout_timeout(), 15000);
        assert_eq!(General::idle_timeout(), 120000);

        env::remove_var("PGDOG_HEALTHCHECK_INTERVAL");
        env::remove_var("PGDOG_HEALTHCHECK_TIMEOUT");
        env::remove_var("PGDOG_CONNECT_TIMEOUT");
        env::remove_var("PGDOG_CHECKOUT_TIMEOUT");
        env::remove_var("PGDOG_IDLE_TIMEOUT");

        assert_eq!(General::healthcheck_interval(), 30000);
        assert_eq!(General::healthcheck_timeout(), 5000);
        assert_eq!(General::default_connect_timeout(), 5000);
        assert_eq!(General::checkout_timeout(), 5000);
        assert_eq!(General::idle_timeout(), 60000);
    }

    #[test]
    fn test_env_invalid_values() {
        env::set_var("PGDOG_WORKERS", "invalid");
        env::set_var("PGDOG_DEFAULT_POOL_SIZE", "not_a_number");

        assert_eq!(General::workers(), 2);
        assert_eq!(General::default_pool_size(), 10);

        env::remove_var("PGDOG_WORKERS");
        env::remove_var("PGDOG_DEFAULT_POOL_SIZE");
    }

    #[test]
    fn test_env_host_port() {
        // Test existing env var functionality
        env::set_var("PGDOG_HOST", "192.168.1.1");
        env::set_var("PGDOG_PORT", "8432");

        assert_eq!(General::host(), "192.168.1.1");
        assert_eq!(General::port(), 8432);

        env::remove_var("PGDOG_HOST");
        env::remove_var("PGDOG_PORT");

        assert_eq!(General::host(), "0.0.0.0");
        assert_eq!(General::port(), 6432);
    }

    #[test]
    fn test_env_enum_fields() {
        // Test pooler mode
        env::set_var("PGDOG_POOLER_MODE", "session");
        assert_eq!(General::pooler_mode(), PoolerMode::Session);
        env::remove_var("PGDOG_POOLER_MODE");
        assert_eq!(General::pooler_mode(), PoolerMode::Transaction);

        // Test load balancing strategy
        env::set_var("PGDOG_LOAD_BALANCING_STRATEGY", "round_robin");
        assert_eq!(
            General::load_balancing_strategy(),
            LoadBalancingStrategy::RoundRobin
        );
        env::remove_var("PGDOG_LOAD_BALANCING_STRATEGY");
        assert_eq!(
            General::load_balancing_strategy(),
            LoadBalancingStrategy::Random
        );

        // Test read-write strategy
        env::set_var("PGDOG_READ_WRITE_STRATEGY", "aggressive");
        assert_eq!(
            General::read_write_strategy(),
            ReadWriteStrategy::Aggressive
        );
        env::remove_var("PGDOG_READ_WRITE_STRATEGY");
        assert_eq!(
            General::read_write_strategy(),
            ReadWriteStrategy::Conservative
        );

        // Test read-write split
        env::set_var("PGDOG_READ_WRITE_SPLIT", "exclude_primary");
        assert_eq!(General::read_write_split(), ReadWriteSplit::ExcludePrimary);
        env::remove_var("PGDOG_READ_WRITE_SPLIT");
        assert_eq!(General::read_write_split(), ReadWriteSplit::IncludePrimary);

        // Test TLS verify mode
        env::set_var("PGDOG_TLS_VERIFY", "verify_full");
        assert_eq!(General::default_tls_verify(), TlsVerifyMode::VerifyFull);
        env::remove_var("PGDOG_TLS_VERIFY");
        assert_eq!(General::default_tls_verify(), TlsVerifyMode::Prefer);

        // Test prepared statements
        env::set_var("PGDOG_PREPARED_STATEMENTS", "full");
        assert_eq!(General::prepared_statements(), PreparedStatements::Full);
        env::remove_var("PGDOG_PREPARED_STATEMENTS");
        assert_eq!(General::prepared_statements(), PreparedStatements::Extended);

        // Test auth type
        env::set_var("PGDOG_AUTH_TYPE", "md5");
        assert_eq!(General::auth_type(), AuthType::Md5);
        env::remove_var("PGDOG_AUTH_TYPE");
        assert_eq!(General::auth_type(), AuthType::Scram);
    }

    #[test]
    fn test_env_additional_timeouts() {
        env::set_var("PGDOG_IDLE_HEALTHCHECK_INTERVAL", "45000");
        env::set_var("PGDOG_IDLE_HEALTHCHECK_DELAY", "10000");
        env::set_var("PGDOG_BAN_TIMEOUT", "600000");
        env::set_var("PGDOG_ROLLBACK_TIMEOUT", "10000");
        env::set_var("PGDOG_SHUTDOWN_TIMEOUT", "120000");
        env::set_var("PGDOG_CONNECT_ATTEMPT_DELAY", "1000");
        env::set_var("PGDOG_QUERY_TIMEOUT", "30000");
        env::set_var("PGDOG_CLIENT_IDLE_TIMEOUT", "3600000");

        assert_eq!(General::idle_healthcheck_interval(), 45000);
        assert_eq!(General::idle_healthcheck_delay(), 10000);
        assert_eq!(General::ban_timeout(), 600000);
        assert_eq!(General::rollback_timeout(), 10000);
        assert_eq!(General::default_shutdown_timeout(), 120000);
        assert_eq!(General::default_connect_attempt_delay(), 1000);
        assert_eq!(General::default_query_timeout(), 30000);
        assert_eq!(General::default_client_idle_timeout(), 3600000);

        env::remove_var("PGDOG_IDLE_HEALTHCHECK_INTERVAL");
        env::remove_var("PGDOG_IDLE_HEALTHCHECK_DELAY");
        env::remove_var("PGDOG_BAN_TIMEOUT");
        env::remove_var("PGDOG_ROLLBACK_TIMEOUT");
        env::remove_var("PGDOG_SHUTDOWN_TIMEOUT");
        env::remove_var("PGDOG_CONNECT_ATTEMPT_DELAY");
        env::remove_var("PGDOG_QUERY_TIMEOUT");
        env::remove_var("PGDOG_CLIENT_IDLE_TIMEOUT");

        assert_eq!(General::idle_healthcheck_interval(), 30000);
        assert_eq!(General::idle_healthcheck_delay(), 5000);
        assert_eq!(General::ban_timeout(), 300000);
        assert_eq!(General::rollback_timeout(), 5000);
        assert_eq!(General::default_shutdown_timeout(), 60000);
        assert_eq!(General::default_connect_attempt_delay(), 0);
    }

    #[test]
    fn test_env_path_fields() {
        env::set_var("PGDOG_TLS_CERTIFICATE", "/path/to/cert.pem");
        env::set_var("PGDOG_TLS_PRIVATE_KEY", "/path/to/key.pem");
        env::set_var("PGDOG_TLS_SERVER_CA_CERTIFICATE", "/path/to/ca.pem");
        env::set_var("PGDOG_QUERY_LOG", "/var/log/pgdog/queries.log");

        assert_eq!(
            General::tls_certificate(),
            Some(PathBuf::from("/path/to/cert.pem"))
        );
        assert_eq!(
            General::tls_private_key(),
            Some(PathBuf::from("/path/to/key.pem"))
        );
        assert_eq!(
            General::tls_server_ca_certificate(),
            Some(PathBuf::from("/path/to/ca.pem"))
        );
        assert_eq!(
            General::query_log(),
            Some(PathBuf::from("/var/log/pgdog/queries.log"))
        );

        env::remove_var("PGDOG_TLS_CERTIFICATE");
        env::remove_var("PGDOG_TLS_PRIVATE_KEY");
        env::remove_var("PGDOG_TLS_SERVER_CA_CERTIFICATE");
        env::remove_var("PGDOG_QUERY_LOG");

        assert_eq!(General::tls_certificate(), None);
        assert_eq!(General::tls_private_key(), None);
        assert_eq!(General::tls_server_ca_certificate(), None);
        assert_eq!(General::query_log(), None);
    }

    #[test]
    fn test_env_numeric_fields() {
        env::set_var("PGDOG_BROADCAST_PORT", "7432");
        env::set_var("PGDOG_OPENMETRICS_PORT", "9090");
        env::set_var("PGDOG_PREPARED_STATEMENTS_LIMIT", "1000");
        env::set_var("PGDOG_QUERY_CACHE_LIMIT", "500");
        env::set_var("PGDOG_CONNECT_ATTEMPTS", "3");
        env::set_var("PGDOG_MIRROR_QUEUE", "256");
        env::set_var("PGDOG_MIRROR_EXPOSURE", "0.5");
        env::set_var("PGDOG_DNS_TTL", "60000");
        env::set_var("PGDOG_PUB_SUB_CHANNEL_SIZE", "100");

        assert_eq!(General::broadcast_port(), 7432);
        assert_eq!(General::openmetrics_port(), Some(9090));
        assert_eq!(General::prepared_statements_limit(), 1000);
        assert_eq!(General::query_cache_limit(), 500);
        assert_eq!(General::connect_attempts(), 3);
        assert_eq!(General::mirror_queue(), 256);
        assert_eq!(General::mirror_exposure(), 0.5);
        assert_eq!(General::default_dns_ttl(), Some(60000));
        assert_eq!(General::pub_sub_channel_size(), 100);

        env::remove_var("PGDOG_BROADCAST_PORT");
        env::remove_var("PGDOG_OPENMETRICS_PORT");
        env::remove_var("PGDOG_PREPARED_STATEMENTS_LIMIT");
        env::remove_var("PGDOG_QUERY_CACHE_LIMIT");
        env::remove_var("PGDOG_CONNECT_ATTEMPTS");
        env::remove_var("PGDOG_MIRROR_QUEUE");
        env::remove_var("PGDOG_MIRROR_EXPOSURE");
        env::remove_var("PGDOG_DNS_TTL");
        env::remove_var("PGDOG_PUB_SUB_CHANNEL_SIZE");

        assert_eq!(General::broadcast_port(), General::port() + 1);
        assert_eq!(General::openmetrics_port(), None);
        assert_eq!(General::prepared_statements_limit(), usize::MAX);
        assert_eq!(General::query_cache_limit(), usize::MAX);
        assert_eq!(General::connect_attempts(), 1);
        assert_eq!(General::mirror_queue(), 128);
        assert_eq!(General::mirror_exposure(), 1.0);
        assert_eq!(General::default_dns_ttl(), None);
        assert_eq!(General::pub_sub_channel_size(), 0);
    }

    #[test]
    fn test_env_boolean_fields() {
        env::set_var("PGDOG_DRY_RUN", "true");
        env::set_var("PGDOG_CROSS_SHARD_DISABLED", "yes");
        env::set_var("PGDOG_LOG_CONNECTIONS", "false");
        env::set_var("PGDOG_LOG_DISCONNECTIONS", "0");

        assert_eq!(General::dry_run(), true);
        assert_eq!(General::cross_shard_disabled(), true);
        assert_eq!(General::log_connections(), false);
        assert_eq!(General::log_disconnections(), false);

        env::remove_var("PGDOG_DRY_RUN");
        env::remove_var("PGDOG_CROSS_SHARD_DISABLED");
        env::remove_var("PGDOG_LOG_CONNECTIONS");
        env::remove_var("PGDOG_LOG_DISCONNECTIONS");

        assert_eq!(General::dry_run(), false);
        assert_eq!(General::cross_shard_disabled(), false);
        assert_eq!(General::log_connections(), true);
        assert_eq!(General::log_disconnections(), true);
    }

    #[test]
    fn test_env_other_fields() {
        env::set_var("PGDOG_BROADCAST_ADDRESS", "192.168.1.100");
        env::set_var("PGDOG_OPENMETRICS_NAMESPACE", "pgdog_metrics");

        assert_eq!(
            General::broadcast_address(),
            Some("192.168.1.100".parse().unwrap())
        );
        assert_eq!(
            General::openmetrics_namespace(),
            Some("pgdog_metrics".to_string())
        );

        env::remove_var("PGDOG_BROADCAST_ADDRESS");
        env::remove_var("PGDOG_OPENMETRICS_NAMESPACE");

        assert_eq!(General::broadcast_address(), None);
        assert_eq!(General::openmetrics_namespace(), None);
    }

    #[test]
    fn test_env_invalid_enum_values() {
        env::set_var("PGDOG_POOLER_MODE", "invalid_mode");
        env::set_var("PGDOG_AUTH_TYPE", "not_an_auth");
        env::set_var("PGDOG_TLS_VERIFY", "bad_verify");

        // Should fall back to defaults for invalid values
        assert_eq!(General::pooler_mode(), PoolerMode::Transaction);
        assert_eq!(General::auth_type(), AuthType::Scram);
        assert_eq!(General::default_tls_verify(), TlsVerifyMode::Prefer);

        env::remove_var("PGDOG_POOLER_MODE");
        env::remove_var("PGDOG_AUTH_TYPE");
        env::remove_var("PGDOG_TLS_VERIFY");
    }

    #[test]
    fn test_general_default_uses_env_vars() {
        // Set some environment variables
        env::set_var("PGDOG_WORKERS", "8");
        env::set_var("PGDOG_POOLER_MODE", "session");
        env::set_var("PGDOG_AUTH_TYPE", "trust");
        env::set_var("PGDOG_DRY_RUN", "true");

        let general = General::default();

        assert_eq!(general.workers, 8);
        assert_eq!(general.pooler_mode, PoolerMode::Session);
        assert_eq!(general.auth_type, AuthType::Trust);
        assert_eq!(general.dry_run, true);

        env::remove_var("PGDOG_WORKERS");
        env::remove_var("PGDOG_POOLER_MODE");
        env::remove_var("PGDOG_AUTH_TYPE");
        env::remove_var("PGDOG_DRY_RUN");
    }
}

//--------------------------------------------------------------------------------------------------
//--------------------------------------------------------------------------------------------------
