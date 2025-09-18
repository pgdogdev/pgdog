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
    /// GSSAPI keytab path for backend authentication.
    pub gssapi_keytab: Option<String>,
    /// GSSAPI principal for backend authentication.
    pub gssapi_principal: Option<String>,
    /// GSSAPI target service principal (what we authenticate to).
    pub gssapi_target_principal: Option<String>,
}

impl Address {
    /// Create new address from config values.
    pub fn new(database: &Database, user: &User) -> Self {
        let cfg = config();

        // Determine GSSAPI settings (database-specific or global defaults)
        let (gssapi_keytab, gssapi_principal) = if let Some(ref gssapi_config) = cfg.config.gssapi {
            if gssapi_config.enabled {
                let keytab = database.gssapi_keytab.clone().or_else(|| {
                    gssapi_config
                        .default_backend_keytab
                        .as_ref()
                        .map(|p| p.to_string_lossy().to_string())
                });
                let principal = database
                    .gssapi_principal
                    .clone()
                    .or_else(|| gssapi_config.default_backend_principal.clone());
                (keytab, principal)
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        // Clean precedence resolution - no parsing needed
        let username = if let Some(user) = database.user.clone() {
            user
        } else if let Some(user) = user.server_user.clone() {
            user
        } else {
            user.name.clone()
        };

        // Target principal precedence: user > database > global default
        let gssapi_target_principal = user
            .gssapi_target_principal
            .clone()
            .or_else(|| database.gssapi_target_principal.clone())
            .or_else(|| {
                cfg.config
                    .gssapi
                    .as_ref()
                    .and_then(|g| g.default_backend_target_principal.clone())
            });

        Address {
            host: database.host.clone(),
            port: database.port,
            database_name: if let Some(database_name) = database.database_name.clone() {
                database_name
            } else {
                database.name.clone()
            },
            user: username,
            password: if let Some(password) = database.password.clone() {
                password
            } else if let Some(password) = user.server_password.clone() {
                password
            } else {
                user.password().to_string()
            },
            gssapi_keytab,
            gssapi_principal,
            gssapi_target_principal,
        }
    }

    /// Check if this address has GSSAPI configuration.
    pub fn has_gssapi(&self) -> bool {
        self.gssapi_keytab.is_some() && self.gssapi_principal.is_some()
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
            .ok_or(Error::DnsResolutionFailed(self.host.clone()))
    }

    #[cfg(test)]
    pub fn new_test() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "pgdog".into(),
            password: "pgdog".into(),
            database_name: "pgdog".into(),
            gssapi_keytab: None,
            gssapi_principal: None,
            gssapi_target_principal: None,
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
            gssapi_keytab: None,
            gssapi_principal: None,
            gssapi_target_principal: None,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::{set, ConfigAndUsers, GssapiConfig};
    use std::path::PathBuf;

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
        assert_eq!(addr.gssapi_keytab, None);
        assert_eq!(addr.gssapi_principal, None);
        assert_eq!(addr.gssapi_target_principal, None);
    }

    #[test]
    fn test_gssapi_config_in_address() {
        // Set up config with GSSAPI
        let mut config = ConfigAndUsers::default();
        config.config.gssapi = Some(GssapiConfig {
            enabled: true,
            server_keytab: Some(PathBuf::from("/etc/pgdog/pgdog.keytab")),
            default_backend_keytab: Some(PathBuf::from("/etc/pgdog/backend.keytab")),
            default_backend_principal: Some("pgdog@REALM".to_string()),
            default_backend_target_principal: Some("postgres/default@REALM".to_string()),
            ..Default::default()
        });

        // Database with specific GSSAPI settings
        let database1 = Database {
            name: "shard1".into(),
            host: "pg1.example.com".into(),
            port: 5432,
            gssapi_keytab: Some("/etc/pgdog/shard1.keytab".into()),
            gssapi_principal: Some("pgdog-shard1@REALM".into()),
            gssapi_target_principal: Some("postgres/shard1@REALM".into()),
            ..Default::default()
        };

        // Database using defaults
        let database2 = Database {
            name: "shard2".into(),
            host: "pg2.example.com".into(),
            port: 5432,
            ..Default::default()
        };

        let user = User {
            name: "testuser".into(),
            database: "shard1".into(),
            ..Default::default()
        };

        // Store the config so Address::new can access it
        set(config).unwrap();

        // Test database with specific GSSAPI settings
        let addr1 = Address::new(&database1, &user);
        assert_eq!(addr1.gssapi_keytab, Some("/etc/pgdog/shard1.keytab".into()));
        assert_eq!(addr1.gssapi_principal, Some("pgdog-shard1@REALM".into()));
        assert_eq!(
            addr1.gssapi_target_principal,
            Some("postgres/shard1@REALM".into())
        );

        // Test database using default GSSAPI settings
        let addr2 = Address::new(&database2, &user);
        assert_eq!(
            addr2.gssapi_keytab,
            Some("/etc/pgdog/backend.keytab".into())
        );
        assert_eq!(addr2.gssapi_principal, Some("pgdog@REALM".into()));
        assert_eq!(
            addr2.gssapi_target_principal,
            Some("postgres/default@REALM".into())
        );
    }

    #[test]
    fn test_gssapi_disabled() {
        // Set up config with GSSAPI disabled
        let mut config = ConfigAndUsers::default();
        config.config.gssapi = Some(GssapiConfig {
            enabled: false,
            server_keytab: Some(PathBuf::from("/etc/pgdog/pgdog.keytab")),
            default_backend_keytab: Some(PathBuf::from("/etc/pgdog/backend.keytab")),
            ..Default::default()
        });

        let database = Database {
            name: "test".into(),
            host: "localhost".into(),
            port: 5432,
            gssapi_keytab: Some("/etc/pgdog/test.keytab".into()),
            ..Default::default()
        };

        let user = User {
            name: "testuser".into(),
            database: "test".into(),
            ..Default::default()
        };

        set(config).unwrap();

        // When GSSAPI is disabled, keytab/principal/target_principal should be None
        let addr = Address::new(&database, &user);
        assert_eq!(addr.gssapi_keytab, None);
        assert_eq!(addr.gssapi_principal, None);
        assert_eq!(addr.gssapi_target_principal, None);
    }
}
