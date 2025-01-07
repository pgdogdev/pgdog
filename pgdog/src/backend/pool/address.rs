//! Server address.

use serde::{Deserialize, Serialize};

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
}

impl Address {
    /// Create new address from config values.
    pub fn new(database: &Database, user: &User) -> Self {
        Address {
            host: database.host.clone(),
            port: database.port,
            database_name: if let Some(database_name) = database.database_name.clone() {
                database_name
            } else {
                database.name.clone()
            },
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
