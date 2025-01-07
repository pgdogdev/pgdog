//! Server address.

use serde::{Deserialize, Serialize};

use super::Config;
use crate::config::{Database, General, User};

/// Server address.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Address {
    /// Server host.
    pub host: String,
    /// Server port.
    pub port: u16,
    /// PostgreSQL database name.
    pub database_name: String,
    /// Username.
    pub user: String,
    /// Password.
    pub password: String,
    /// Pool configuration.
    pub config: Config,
}

impl Address {
    /// Create new address from config values.
    pub fn new(general: &General, database: &Database, user: &User) -> Self {
        Address {
            host: database.host.clone(),
            port: database.port,
            database_name: database.name.clone(),
            user: if let Some(user) = database.user.clone() {
                user
            } else {
                user.name.clone()
            },
            password: if let Some(password) = database.password.clone() {
                password
            } else {
                user.password.clone()
            },
            config: Config {
                min: general.min_pool_size,
                max: general.default_pool_size,
                healthcheck_interval: general.healthcheck_interval,
                idle_healthcheck_interval: general.idle_healthcheck_interval,
                idle_healthcheck_delay: general.idle_healthcheck_delay,
                ..Default::default()
            },
        }
    }

    /// Pool needs to be re-created on configuration reload.
    pub fn need_recreate(&self, other: &Address) -> bool {
        self.host != other.host || self.port != other.port
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}
