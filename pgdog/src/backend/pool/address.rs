//! Server address.

use serde::{Deserialize, Serialize};

use crate::config::{Database, User};

/// Server address.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
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
            } else if let Some(user) = user.server_user.clone() {
                user
            } else {
                user.name.clone()
            },
            password: if let Some(password) = database.password.clone() {
                password
            } else if let Some(password) = user.server_password.clone() {
                password
            } else {
                user.password().to_string()
            },
        }
    }

    /// Get address for `TCPStream`.
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}, {}", self.host, self.port, self.database_name)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_defaults() {
        let mut database = Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 6432,
            ..Default::default()
        };

        let user = User {
            name: "pgdog".into(),
            password: Some("hunter2".into()),
            database: "pgdog".into(),
            ..Default::default()
        };

        let address = Address::new(&database, &user);

        assert_eq!(address.host, "127.0.0.1");
        assert_eq!(address.port, 6432);
        assert_eq!(address.database_name, "pgdog");
        assert_eq!(address.user, "pgdog");
        assert_eq!(address.password, "hunter2");

        database.database_name = Some("not_pgdog".into());
        database.password = Some("hunter3".into());
        database.user = Some("alice".into());

        let address = Address::new(&database, &user);

        assert_eq!(address.database_name, "not_pgdog");
        assert_eq!(address.user, "alice");
        assert_eq!(address.password, "hunter3");
    }
}
