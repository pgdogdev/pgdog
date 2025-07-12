//! Server address.
use std::net::{SocketAddr, ToSocketAddrs};

use serde::{Deserialize, Serialize};
use url::Url;

use crate::backend::{pool::dns_cache::DnsCache, Error};
use crate::config::{config, Database, User};

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

    pub async fn addr(&self) -> Result<SocketAddr, Error> {
        let dns_cache_override_enabled = config().config.general.dns_ttl().is_some();

        if dns_cache_override_enabled {
            let ip = DnsCache::global().resolve(&self.host).await?;
            return Ok(SocketAddr::new(ip, self.port));
        }

        let addr_str = format!("{}:{}", self.host, self.port);
        let mut socket_addrs = addr_str.to_socket_addrs()?;

        socket_addrs
            .next()
            .ok_or(Error::DnsLookupError(self.host.clone()))
    }

    #[cfg(test)]
    pub fn new_test() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "pgdog".into(),
            password: "pgdog".into(),
            database_name: "pgdog".into(),
        }
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}, {}", self.host, self.port, self.database_name)
    }
}

impl TryFrom<Url> for Address {
    type Error = ();

    fn try_from(value: Url) -> Result<Self, Self::Error> {
        let host = value.host().ok_or(())?.to_string();
        let port = value.port().unwrap_or(5432);
        let user = value.username().to_string();
        let password = value.password().ok_or(())?.to_string();
        let database_name = value.path().replace("/", "").to_string();

        Ok(Self {
            host,
            port,
            password,
            user,
            database_name,
        })
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

    #[test]
    fn test_addr_from_url() {
        let addr =
            Address::try_from(Url::parse("postgres://user:password@127.0.0.1:6432/pgdb").unwrap())
                .unwrap();
        assert_eq!(addr.host, "127.0.0.1");
        assert_eq!(addr.port, 6432);
        assert_eq!(addr.database_name, "pgdb");
        assert_eq!(addr.user, "user");
        assert_eq!(addr.password, "password");
    }
}
