//! Configuration.

pub mod error;

use error::Error;

use std::fs::read_to_string;
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tracing::info;

static CONFIG: Lazy<ArcSwap<ConfigAndUsers>> =
    Lazy::new(|| ArcSwap::from_pointee(ConfigAndUsers::default()));

/// Load configuration.
pub fn config() -> Arc<ConfigAndUsers> {
    CONFIG.load().clone()
}

/// Load the configuration file from disk.
pub fn load(config: &PathBuf, users: &PathBuf) -> Result<ConfigAndUsers, Error> {
    let config = ConfigAndUsers::load(config, users)?;
    CONFIG.store(Arc::new(config.clone()));
    Ok(config)
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
            info!("loaded pgdog.toml");
            config
        } else {
            Config::default()
        };

        let users: Users = if let Ok(users) = read_to_string(users_path) {
            let users = toml::from_str(&users)?;
            info!("loaded users.toml");
            users
        } else {
            Users::default()
        };

        Ok(ConfigAndUsers {
            config,
            users,
            config_path: config_path.to_owned(),
            users_path: users_path.to_owned(),
        })
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
    /// Servers.
    #[serde(default)]
    pub databases: Vec<Database>,
    #[serde(default)]
    pub plugins: Vec<Plugin>,
}

impl Config {
    /// Organize all databases by name for quicker retrival.
    pub fn databases(&self) -> HashMap<String, Vec<Database>> {
        let mut databases = HashMap::new();
        for database in &self.databases {
            let entry = databases
                .entry(database.name.clone())
                .or_insert_with(Vec::new);
            entry.push(database.clone());
        }
        databases
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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
}

impl General {
    fn host() -> String {
        "0.0.0.0".into()
    }

    fn port() -> u16 {
        6432
    }

    fn workers() -> usize {
        0
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
        5 * 60_000
    }

    fn rollback_timeout() -> u64 {
        5_000
    }

    fn load_balancing_strategy() -> LoadBalancingStrategy {
        LoadBalancingStrategy::Random
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Stats {}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PoolerMode {
    #[default]
    Transaction,
    Session,
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
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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
    /// PostgreSQL database name, e.g. "postgres".
    pub database_name: Option<String>,
    /// Use this user to connect to the database, overriding the userlist.
    pub user: Option<String>,
    /// Use this password to login, overriding the userlist.
    pub password: Option<String>,
    // Maximum number of connections to this database from this pooler.
    // #[serde(default = "Database::max_connections")]
    // pub max_connections: usize,
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

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    #[default]
    Primary,
    Replica,
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
}

/// User allowed to connect to pgDog.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct User {
    /// User name.
    pub name: String,
    /// Database name, from pgdog.toml.
    pub database: String,
    /// User's password.
    pub password: String,
    /// Pool size for this user pool, overriding `default_pool_size`.
    pub pool_size: Option<usize>,
    /// Pooler mode.
    pub pooler_mode: Option<PoolerMode>,
    /// Server username.
    pub server_user: Option<String>,
    /// Server password.
    pub server_password: Option<String>,
}

impl User {}

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

[[plugins]]
name = "pgdog_routing"
"#;

        let config: Config = toml::from_str(source).unwrap();
        assert_eq!(config.databases[0].name, "production");
        assert_eq!(config.plugins[0].name, "pgdog_routing");
    }
}
