//! Server address.
use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::Deref;

use pgdog_config::Role;
use pgdog_config::users::PasswordKind;
use serde::{Deserialize, Serialize};
use url::Url;

use super::{Password, password::PasswordSource};
use crate::backend::Error;
use crate::backend::auth::{azure_workload_identity, rds_iam, vault};
use crate::backend::pool::dns_cache::DnsCache;
use crate::backend::pool::token_cache::TokenCache;
use crate::config::{Database, ServerAuth, User, config};

/// Server address.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, Eq, Hash)]
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
    pub passwords: Vec<Password>,
    /// Server auth mode for backend connections.
    #[serde(default)]
    pub server_auth: ServerAuth,
    /// Optional IAM region override.
    pub server_iam_region: Option<String>,
    /// Vault path to fetch dynamic credentials from.
    #[serde(default)]
    pub vault_path: Option<String>,
    /// Percentage of the Vault lease after which credentials are refreshed.
    #[serde(default)]
    pub vault_refresh_percent: Option<u8>,
    /// Database number (in the config).
    pub database_number: usize,
    /// Role given to the database at configuration time.
    /// For automatic roles, this can change at runtime.
    pub configured_role: Role,
}

impl From<Address> for pgdog_stats::Address {
    fn from(value: Address) -> Self {
        pgdog_stats::Address {
            host: value.host,
            port: value.port,
            database_name: value.database_name,
            user: value.user,
            passwords: value.passwords.iter().map(|p| p.deref().clone()).collect(),
            server_auth: value.server_auth,
            server_iam_region: value.server_iam_region,
            database_number: value.database_number,
        }
    }
}

impl Address {
    /// Create new address from config values.
    pub fn new(database: &Database, user: &User, database_number: usize) -> Self {
        let server_auth = user.server_auth;

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
            passwords: if server_auth.is_external_identity() {
                vec![]
            } else if let Some(password) = database.password.clone() {
                vec![password.into()]
            } else if let Some(password) = user.server_password.clone() {
                vec![password.into()]
            } else {
                user.passwords()
                    .into_iter()
                    .filter(|p| matches!(p, PasswordKind::Plain(_)))
                    .map(|p| p.to_string().into())
                    .collect()
            },
            server_auth,
            server_iam_region: user.server_iam_region.clone(),
            vault_path: user.server_vault_path.clone(),
            vault_refresh_percent: user.vault_refresh_percent,
            database_number,
            configured_role: database.role,
        }
    }

    /// Get address passwords, in valid order.
    ///
    /// For external identity providers the token is served from the global
    /// [`TokenCache`], which is kept warm by the pool monitor. This call
    /// only blocks on the very first connection before the monitor has had
    /// a chance to prime the cache.
    pub async fn auth_secrets(&self) -> Result<Vec<Password>, Error> {
        Ok(self.auth_credentials().await?.1)
    }

    /// Get the username and passwords to log into the server with.
    ///
    /// The username is the configured one, except for `vault_dynamic` pools
    /// where the database secrets engine generates it together with the
    /// password.
    pub async fn auth_credentials(&self) -> Result<(String, Vec<Password>), Error> {
        let mut user = self.user.clone();

        let mut secrets = match self.server_auth {
            ServerAuth::Password => self.passwords.clone(),

            ServerAuth::RdsIam => {
                let token = TokenCache::global()
                    .get_or_fetch(self, rds_iam::token)
                    .await?;
                vec![Password::new(&token, PasswordSource::RdsIam)]
            }

            ServerAuth::AzureWorkloadIdentity => {
                let token = TokenCache::global()
                    .get_or_fetch(self, azure_workload_identity::token)
                    .await?;
                vec![Password::new(&token, PasswordSource::AzureIdentity)]
            }

            ServerAuth::VaultDynamic => {
                let credentials = TokenCache::global()
                    .credentials_or_fetch(self, vault::credentials)
                    .await?;
                if let Some(username) = credentials.username {
                    user = username;
                }
                vec![Password::new(&credentials.secret, PasswordSource::Vault)]
            }

            ServerAuth::VaultStatic => {
                let password = TokenCache::global()
                    .get_or_fetch_with_refresh(self, vault::static_backend_credentials)
                    .await?;
                vec![Password::new(&password, PasswordSource::Vault)]
            }
        };

        // Give the valid password first.
        secrets.sort_by_cached_key(|p| !p.is_valid());

        Ok((user, secrets))
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

    /// Test convention: `new_test()` represents a primary. Tests that need
    /// a replica do `Address { configured_role: Role::Replica, ..new_test() }`.
    #[cfg(test)]
    pub fn new_test() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "pgdog".into(),
            passwords: vec!["pgdog".into()],
            database_name: "pgdog".into(),
            server_auth: ServerAuth::Password,
            server_iam_region: None,
            vault_path: None,
            vault_refresh_percent: None,
            database_number: 0,
            configured_role: Role::Primary,
        }
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}@{}:{}/{}",
            self.user, self.host, self.port, self.database_name
        )
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

        // A URL says nothing about role; fall through to `Role::Auto`
        // via the derived `Default`. The PROBE command (the only caller)
        // never reads `configured_role` anyway.
        Ok(Self {
            host,
            port,
            passwords: vec![password.into()],
            user,
            database_name,
            server_auth: ServerAuth::Password,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod test {
    use std::time::{Duration, SystemTime};

    use crate::config;

    use super::*;

    // ── Address::new ─────────────────────────────────────────────────────────

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

        let address = Address::new(&database, &user, 0);

        assert_eq!(address.host, "127.0.0.1");
        assert_eq!(address.port, 6432);
        assert_eq!(address.database_name, "pgdog");
        assert_eq!(address.user, "pgdog");
        assert_eq!(address.passwords.first().unwrap(), "hunter2");

        database.database_name = Some("not_pgdog".into());
        database.password = Some("hunter3".into());
        database.user = Some("alice".into());

        let address = Address::new(&database, &user, 0);

        assert_eq!(address.database_name, "not_pgdog");
        assert_eq!(address.user, "alice");
        assert_eq!(address.passwords.first().unwrap(), "hunter3");
    }

    #[test]
    fn test_rds_iam_does_not_use_static_password() {
        let database = Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 6432,
            password: Some("db-level-pass".into()),
            ..Default::default()
        };

        let user = User {
            name: "pgdog".into(),
            password: Some("user-pass".into()),
            server_password: Some("server-pass".into()),
            server_auth: ServerAuth::RdsIam,
            server_iam_region: Some("us-east-1".into()),
            database: "pgdog".into(),
            ..Default::default()
        };

        let address = Address::new(&database, &user, 0);
        assert!(
            address.passwords.is_empty(),
            "RDS IAM addresses must not carry static passwords"
        );
        assert_eq!(address.server_auth, ServerAuth::RdsIam);
        assert_eq!(address.server_iam_region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn test_azure_workload_identity_does_not_use_static_password() {
        let database = Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 6432,
            password: Some("db-level-pass".into()),
            ..Default::default()
        };

        let user = User {
            name: "pgdog".into(),
            password: Some("user-pass".into()),
            server_password: Some("server-pass".into()),
            server_auth: ServerAuth::AzureWorkloadIdentity,
            server_iam_region: None,
            database: "pgdog".into(),
            ..Default::default()
        };

        let address = Address::new(&database, &user, 0);
        assert!(
            address.passwords.is_empty(),
            "Azure Workload Identity addresses must not carry static passwords"
        );
        assert_eq!(address.server_auth, ServerAuth::AzureWorkloadIdentity);
    }

    // ── TryFrom<Url> ─────────────────────────────────────────────────────────

    #[test]
    fn test_addr_from_url() {
        let addr =
            Address::try_from(Url::parse("postgres://user:password@127.0.0.1:6432/pgdb").unwrap())
                .unwrap();
        assert_eq!(addr.host, "127.0.0.1");
        assert_eq!(addr.port, 6432);
        assert_eq!(addr.database_name, "pgdb");
        assert_eq!(addr.user, "user");
        assert_eq!(addr.passwords.first().unwrap(), "password");
        assert_eq!(addr.server_auth, ServerAuth::Password);
        assert!(addr.server_iam_region.is_none());
    }

    // ── auth_secrets: password mode ──────────────────────────────────────────

    #[tokio::test]
    async fn test_auth_secret_password_mode() {
        let addr = Address::new_test();
        assert_eq!(addr.auth_secrets().await.unwrap().first().unwrap(), "pgdog");
    }

    #[tokio::test]
    async fn test_auth_secrets_returns_valid_password_first() {
        let mut addr = Address::new_test();
        let invalid1: Password = "invalid1".into();
        let invalid2: Password = "invalid2".into();
        let valid: Password = "valid".into();
        invalid1.valid(false);
        invalid2.valid(false);
        addr.passwords = vec![invalid1, valid, invalid2];

        let secrets = addr.auth_secrets().await.unwrap();
        assert_eq!(secrets.len(), 3);
        assert_eq!(secrets.first().unwrap(), "valid");
        assert!(secrets.first().unwrap().is_valid());

        // Even if the valid password is last, it should still come first.
        let mut addr = Address::new_test();
        let invalid1: Password = "invalid1".into();
        let invalid2: Password = "invalid2".into();
        let valid: Password = "valid".into();
        invalid1.valid(false);
        invalid2.valid(false);
        addr.passwords = vec![invalid1, invalid2, valid];

        let secrets = addr.auth_secrets().await.unwrap();
        assert_eq!(secrets.first().unwrap(), "valid");

        // With multiple valid passwords, a valid one is still first.
        let mut addr = Address::new_test();
        let invalid: Password = "invalid".into();
        invalid.valid(false);
        addr.passwords = vec![invalid, "valid_a".into(), "valid_b".into()];

        let secrets = addr.auth_secrets().await.unwrap();
        let head = secrets.first().unwrap();
        assert!(head.is_valid());
        assert!(head == "valid_a" || head == "valid_b");

        // Flipping validity at runtime changes which password comes first.
        let mut addr = Address::new_test();
        let first: Password = "first".into();
        let second: Password = "second".into();
        addr.passwords = vec![first.clone(), second.clone()];

        // Both valid: order is preserved (sort is stable on the !is_valid key).
        let secrets = addr.auth_secrets().await.unwrap();
        assert_eq!(secrets.first().unwrap(), "first");
        assert_eq!(secrets.get(1).unwrap(), "second");

        // Mark "first" invalid — "second" must now win.
        first.valid(false);
        let secrets = addr.auth_secrets().await.unwrap();
        assert_eq!(secrets.first().unwrap(), "second");
        assert!(secrets.first().unwrap().is_valid());
        assert_eq!(secrets.get(1).unwrap(), "first");
        assert!(!secrets.get(1).unwrap().is_valid());

        // Mark "second" invalid too — no valid password, but order stays stable.
        second.valid(false);
        let secrets = addr.auth_secrets().await.unwrap();
        assert_eq!(secrets.first().unwrap(), "first");
        assert!(!secrets.first().unwrap().is_valid());
        assert_eq!(secrets.get(1).unwrap(), "second");

        // Restore "first" — it should be returned first again.
        first.valid(true);
        let secrets = addr.auth_secrets().await.unwrap();
        assert_eq!(secrets.first().unwrap(), "first");
        assert!(secrets.first().unwrap().is_valid());
        assert_eq!(secrets.get(1).unwrap(), "second");
        assert!(!secrets.get(1).unwrap().is_valid());
    }

    // ── auth_secrets: external identity ──────────────────────────────────────
    //
    // These tests pre-populate the global token cache rather than hitting real
    // cloud endpoints. Each uses a unique (host, port, user) combination and
    // evicts after the test to avoid cross-test interference.

    #[tokio::test]
    async fn test_auth_secret_rds_iam_serves_token_from_cache() {
        let addr = Address {
            host: "auth-secrets-rds.internal".into(),
            port: 15432,
            user: "rds_user".into(),
            server_auth: ServerAuth::RdsIam,
            server_iam_region: Some("us-east-1".into()),
            ..Default::default()
        };

        let expiry = SystemTime::now() + Duration::from_secs(3600);
        TokenCache::global().set(&addr, "rds-token".into(), expiry);

        let secret = addr
            .auth_secrets()
            .await
            .unwrap()
            .first()
            .unwrap()
            .to_string();

        TokenCache::global().evict(&addr);

        assert_eq!(secret, "rds-token");
    }

    #[tokio::test]
    async fn test_auth_secret_azure_workload_identity_serves_token_from_cache() {
        let addr = Address {
            host: "auth-secrets-azure.internal".into(),
            port: 15433,
            user: "azure_user".into(),
            server_auth: ServerAuth::AzureWorkloadIdentity,
            ..Default::default()
        };

        let expiry = SystemTime::now() + Duration::from_secs(3600);
        TokenCache::global().set(&addr, "azure-token".into(), expiry);

        let secret = addr
            .auth_secrets()
            .await
            .unwrap()
            .first()
            .unwrap()
            .to_string();

        TokenCache::global().evict(&addr);

        assert_eq!(secret, "azure-token");
    }

    #[tokio::test]
    async fn test_auth_credentials_vault_serves_username_and_password_from_cache() {
        use crate::backend::pool::token_cache::{Credentials, FetchedCredentials};

        let addr = Address {
            host: "auth-secrets-vault.internal".into(),
            port: 15435,
            user: "configured_user".into(),
            server_auth: ServerAuth::VaultDynamic,
            vault_path: Some("database/creds/pgdog".into()),
            ..Default::default()
        };

        let now = SystemTime::now();
        TokenCache::global().set_credentials(
            &addr,
            FetchedCredentials {
                credentials: Credentials {
                    username: Some("v-generated-user".into()),
                    secret: "v-generated-pass".into(),
                },
                expires_at: now + Duration::from_secs(3600),
                refresh_at: Some(now + Duration::from_secs(2880)),
            },
        );

        let (user, secrets) = addr.auth_credentials().await.unwrap();

        TokenCache::global().evict(&addr);

        // Vault generates the username — it must override the configured one.
        assert_eq!(user, "v-generated-user");
        assert_eq!(secrets.first().unwrap(), "v-generated-pass");
    }

    #[tokio::test]
    async fn test_auth_credentials_password_mode_uses_configured_user() {
        let addr = Address::new_test();
        let (user, secrets) = addr.auth_credentials().await.unwrap();
        assert_eq!(user, "pgdog");
        assert_eq!(secrets.first().unwrap(), "pgdog");
    }

    #[test]
    fn test_vault_fields_from_config() {
        let database = Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 6432,
            ..Default::default()
        };

        let user = User {
            name: "pgdog".into(),
            server_auth: ServerAuth::VaultDynamic,
            server_vault_path: Some("database/creds/pgdog".into()),
            vault_refresh_percent: Some(50),
            password: Some("ignored".into()),
            database: "pgdog".into(),
            ..Default::default()
        };

        let address = Address::new(&database, &user, 0);
        assert!(
            address.passwords.is_empty(),
            "Vault addresses must not carry static passwords"
        );
        assert_eq!(address.server_auth, ServerAuth::VaultDynamic);
        assert_eq!(address.vault_path.as_deref(), Some("database/creds/pgdog"));
        assert_eq!(address.vault_refresh_percent, Some(50));
    }

    #[tokio::test]
    async fn test_auth_credentials_vault_static_serves_password_from_cache() {
        let addr = Address {
            host: "auth-secrets-vault-static.internal".into(),
            port: 15436,
            user: "pgdog_static".into(),
            server_auth: ServerAuth::VaultStatic,
            vault_path: Some("database/static-creds/pgdog-static".into()),
            ..Default::default()
        };

        let expiry = SystemTime::now() + Duration::from_secs(3600);
        TokenCache::global().set(&addr, "vault-rotated-pass".into(), expiry);

        let (user, secrets) = addr.auth_credentials().await.unwrap();

        TokenCache::global().evict(&addr);

        assert_eq!(user, "pgdog_static");
        assert_eq!(secrets.first().unwrap(), "vault-rotated-pass");
    }

    #[test]
    fn test_vault_static_fields_from_config() {
        let database = Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 6432,
            ..Default::default()
        };

        let user = User {
            name: "pgdog_static".into(),
            server_auth: ServerAuth::VaultStatic,
            server_vault_path: Some("database/static-creds/pgdog-static".into()),
            vault_refresh_percent: Some(70),
            password: Some("ignored".into()),
            database: "pgdog".into(),
            ..Default::default()
        };

        let address = Address::new(&database, &user, 0);
        assert!(
            address.passwords.is_empty(),
            "VaultStatic addresses must not carry static passwords"
        );
        assert_eq!(address.server_auth, ServerAuth::VaultStatic);
        assert_eq!(
            address.vault_path.as_deref(),
            Some("database/static-creds/pgdog-static")
        );
        assert_eq!(address.vault_refresh_percent, Some(70));
    }

    #[tokio::test]
    async fn test_auth_secret_stale_token_still_returned() {
        // A stale token (past its expiry) is still handed to the server.
        // The monitor is responsible for refreshing it; auth_secrets never
        // blocks on a refresh.
        let addr = Address {
            host: "auth-secrets-stale.internal".into(),
            port: 15434,
            user: "stale_user".into(),
            server_auth: ServerAuth::RdsIam,
            server_iam_region: Some("eu-west-1".into()),
            ..Default::default()
        };

        let stale_expiry = SystemTime::now() - Duration::from_secs(60);
        TokenCache::global().set(&addr, "stale-token".into(), stale_expiry);

        let secret = addr
            .auth_secrets()
            .await
            .unwrap()
            .first()
            .unwrap()
            .to_string();

        TokenCache::global().evict(&addr);

        assert_eq!(secret, "stale-token");
    }

    #[tokio::test]
    async fn test_addr_uses_dns_cache_when_dns_ttl_is_configured() {
        let cache = DnsCache::global();
        let hostname = "localhost";

        let mut test_config = (*config::config()).clone();
        test_config.config.general.dns_ttl = Some(60_000);
        config::set(test_config).expect("set dns_ttl");
        cache.clear_cache_for_testing();

        let addr = Address {
            host: hostname.into(),
            port: 15432,
            ..Address::new_test()
        };

        let socket_addr = addr.addr().await.expect("resolve address");

        assert_eq!(socket_addr.port(), addr.port);
        assert_eq!(
            cache.cached_ip_for_testing(hostname),
            Some(socket_addr.ip())
        );
    }
}
