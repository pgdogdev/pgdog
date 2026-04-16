use serde::{Deserialize, Serialize};
use std::env;
use std::fmt::Display;
use std::path::PathBuf;
use tracing::warn;

use super::core::Config;
use super::pooling::PoolerMode;
use crate::util::random_string;
use schemars::JsonSchema;

/// Plugins are dynamically loaded at PgDog startup. These settings control which plugins are loaded.
///
/// Note: Plugins can only be configured at PgDog startup. They cannot be changed after the process is running.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/plugins/
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Plugin {
    /// Name of the plugin to load. This is used by PgDog to look up the shared library object in `LD_LIBRARY_PATH`. For example, if your plugin name is `router`, PgDog will look for `librouter.so` on Linux, `librouter.dll` on Windows, and `librouter.dylib` on Mac OS.
    ///
    /// **Note:** Make sure the user running PgDog has read & execute permissions on the library.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/plugins/#name
    pub name: String,

    /// Path to the configuration file for the plugin, if any. Plugin-specific settings can be
    /// placed there. It's completely plugin-specific and any fomrat is acceptable.
    pub config: Option<PathBuf>,
}

/// This configuration controls which users are allowed to connect to PgDog. This is a TOML list so for each user, add a `[[users]]` section to `users.toml`.
///
/// https://docs.pgdog.dev/configuration/users.toml/users/
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Users {
    /// Admin database configuration.
    pub admin: Option<Admin>,
    /// Users and passwords.
    ///
    /// https://docs.pgdog.dev/configuration/users.toml/users/
    #[serde(default)]
    pub users: Vec<User>,
}

impl Users {
    /// Run configuration checks.
    pub fn check(&mut self, config: &Config) {
        for user in &mut self.users {
            if user.password().is_empty() {
                if !config.general.passthrough_auth() && user.password_hash.is_none() {
                    warn!(
                        "user \"{}\" doesn't have a password and passthrough auth is disabled",
                        user.name
                    );
                }

                if let Some(min_pool_size) = user.min_pool_size {
                    let databases = if user.database.is_empty() {
                        user.databases.clone()
                    } else {
                        vec![user.database.clone()]
                    };

                    for database in databases {
                        if min_pool_size > 0 {
                            warn!("user \"{}\" (database \"{}\") doesn't have a password configured, \
                            so we can't connect to the server to maintain min_pool_size of {}; setting it to 0", user.name, database, min_pool_size);
                            user.min_pool_size = Some(0);
                        }
                    }
                }
            }

            if !user.database.is_empty() && !user.databases.is_empty() {
                warn!(
                    r#"user "{}" is configured for both "database" and "databases", defaulting to "database""#,
                    user.name
                );
            }

            if user.all_databases && (!user.databases.is_empty() || !user.database.is_empty()) {
                warn!(
                    r#"user "{}" is configured for "all_databases" and specific databases, defaulting to "all_databases""#,
                    user.name
                );
            }
        }
    }

    /// Swap user database references between source and destination.
    /// Users on source become users on destination, and vice versa.
    pub fn cutover(&mut self, source: &str, destination: &str) {
        let tmp = format!("__tmp_{}__", random_string(12));

        crate::swap_field!(self.users.iter_mut(), database, source, destination, tmp);
    }

    /// Remove user with the same name and database.
    ///
    /// Add new user in its place.
    pub fn add_or_replace(&mut self, user: User) {
        self.users
            .retain(|existing| !(existing.name == user.name && existing.database == user.database));
        self.users.push(user);
    }

    pub fn find(&self, user: &User) -> Option<User> {
        self.users
            .iter()
            .find(|existing| existing.name == user.name && existing.database == user.database)
            .cloned()
    }
}

/// Backend authentication mode used by PgDog for server connections.
#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ServerAuth {
    /// Use configured static password.
    #[default]
    Password,
    /// Generate an AWS RDS IAM auth token per connection attempt.
    RdsIam,
}

impl ServerAuth {
    pub fn rds_iam(&self) -> bool {
        matches!(self, Self::RdsIam)
    }
}

/// The kind of password configured on the user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PasswordKind {
    Plain(String),
    Hashed(String),
}

impl PasswordKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Plain(plain) => plain.as_str(),
            Self::Hashed(hash) => hash.as_str(),
        }
    }
}

impl Display for PasswordKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(plain) => write!(f, "{}", plain),
            Self::Hashed(hashed) => write!(f, "{}", hashed),
        }
    }
}

/// User allowed to connect to pgDog.
/// A user entry in `users.toml`, controlling which users are allowed to connect to PgDog.
///
/// https://docs.pgdog.dev/configuration/users.toml/users/
#[derive(
    Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, Ord, PartialOrd, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct User {
    /// Name of the user. Clients that connect to PgDog will need to use this username.
    ///
    /// https://docs.pgdog.dev/configuration/users.toml/users/#name
    pub name: String,
    /// Name of the database cluster this user belongs to. This refers to `name` setting in [`pgdog.toml`](https://docs.pgdog.dev/configuration/pgdog.toml/databases/), databases section.
    ///
    /// https://docs.pgdog.dev/configuration/users.toml/users/#database
    #[serde(default)]
    pub database: String,
    /// List of database clusters this user has access to.
    #[serde(default)]
    pub databases: Vec<String>,
    /// User belongs to all databases.
    #[serde(default)]
    pub all_databases: bool,
    /// The password for the user. Clients will need to provide this when connecting to PgDog.
    ///
    /// https://docs.pgdog.dev/configuration/users.toml/users/#password
    pub password: Option<String>,
    /// Multiple passwords for this user, all of which will be attempted during auth to server and client.
    #[serde(default)]
    pub passwords: Vec<String>,
    /// Passwords hash. Can be used to validate user logins without storing passwords in users.toml.
    /// Server authentication must use RDS IAM or some other passwordless authentication, e.g. trust.
    pub password_hash: Option<String>,
    /// Overrides [`default_pool_size`](https://docs.pgdog.dev/configuration/pgdog.toml/general/) for this user. No more than this many server connections will be open at any given time to serve requests for this connection pool.
    ///
    /// https://docs.pgdog.dev/configuration/users.toml/users/#pool_size
    pub pool_size: Option<usize>,
    /// Overrides [`min_pool_size`](https://docs.pgdog.dev/configuration/pgdog.toml/general/#min_pool_size) for this user. Opens at least this many connections on pooler startup and keeps them open despite [`idle_timeout`](https://docs.pgdog.dev/configuration/pgdog.toml/general/#idle_timeout).
    ///
    /// https://docs.pgdog.dev/configuration/users.toml/users/#min_pool_size
    pub min_pool_size: Option<usize>,
    /// Overrides [`pooler_mode`](https://docs.pgdog.dev/configuration/pgdog.toml/general/) for this user. This allows users in [session mode](https://docs.pgdog.dev/features/session-mode/) to connect to the same PgDog instance as users in [transaction mode](https://docs.pgdog.dev/features/transaction-mode/).
    ///
    /// https://docs.pgdog.dev/configuration/users.toml/users/#pooler_mode
    pub pooler_mode: Option<PoolerMode>,
    /// Which user to connect with when creating backend connections from PgDog to PostgreSQL. By default, the user configured in `name` is used. This setting allows you to override this configuration and use a different user.
    ///
    /// **Note:** Values specified in `pgdog.toml` take priority over this configuration.
    ///
    /// https://docs.pgdog.dev/configuration/users.toml/users/#server_user
    pub server_user: Option<String>,
    /// Which password to connect with when creating backend connections from PgDog to PostgreSQL. By default, the password configured in `password` is used. This setting allows you to override this configuration and use a different password, decoupling server passwords from user passwords given to clients.
    ///
    /// **Note:** Values specified in `pgdog.toml` take priority over this configuration.
    ///
    /// https://docs.pgdog.dev/configuration/users.toml/users/#server_password
    pub server_password: Option<String>,
    /// Backend auth mode for server connections.
    #[serde(default)]
    pub server_auth: ServerAuth,
    /// Optional region override for RDS IAM token generation.
    pub server_iam_region: Option<String>,
    /// Statement timeout.
    ///
    /// Sets the `statement_timeout` on all server connections at connection creation. This allows you to set a reasonable default for each user without modifying `postgresql.conf` or using `ALTER USER`.
    ///
    /// **Note:** Nothing is preventing the user from manually changing this setting at runtime, e.g., by running `SET statement_timeout TO 0`;
    ///
    /// https://docs.pgdog.dev/configuration/users.toml/users/#statement_timeout
    pub statement_timeout: Option<u64>,
    /// Sets the `replication=database` parameter on user connections to Postgres. Allows this user to use replication commands.
    ///
    /// _Default:_ `false`
    ///
    /// https://docs.pgdog.dev/configuration/users.toml/users/#replication_mode
    #[serde(default)]
    pub replication_mode: bool,
    /// Sharding target database for replication.
    pub replication_sharding: Option<String>,
    /// Overrides [`idle_timeout`](https://docs.pgdog.dev/configuration/pgdog.toml/general/#idle_timeout) for this user. Server connections that have been idle for this long, without affecting [`min_pool_size`](https://docs.pgdog.dev/configuration/pgdog.toml/general/#min_pool_size), will be closed.
    ///
    /// https://docs.pgdog.dev/configuration/users.toml/users/#idle_timeout
    pub idle_timeout: Option<u64>,
    /// Sets `default_transaction_read_only` to `on` for all connections.
    pub read_only: Option<bool>,
    /// Schema owner with elevated DDL privileges.
    #[serde(default)]
    pub schema_admin: bool,
    /// Disable cross-shard queries for this user.
    pub cross_shard_disabled: Option<bool>,
    /// Overrides [`two_phase_commit`](https://docs.pgdog.dev/configuration/pgdog.toml/general/#two_phase_commit) for this user.
    ///
    /// https://docs.pgdog.dev/configuration/users.toml/users/#two_phase_commit
    pub two_phase_commit: Option<bool>,
    /// Overrides [`two_phase_commit_auto`](https://docs.pgdog.dev/configuration/pgdog.toml/general/#two_phase_commit_auto) for this user.
    ///
    /// https://docs.pgdog.dev/configuration/users.toml/users/#two_phase_commit_auto
    pub two_phase_commit_auto: Option<bool>,
    /// Server connections older than this (in milliseconds) will be closed when returned to the pool.
    pub server_lifetime: Option<u64>,
}

impl User {
    fn password(&self) -> &str {
        if let Some(ref s) = self.password {
            s.as_str()
        } else {
            ""
        }
    }

    pub fn passwords(&self) -> Vec<PasswordKind> {
        let mut passwords: Vec<_> = self
            .passwords
            .clone()
            .into_iter()
            .map(|p| PasswordKind::Plain(p))
            .collect();
        if !self.password().is_empty() {
            passwords.push(PasswordKind::Plain(self.password().to_string()));
        }

        if let Some(hash) = self.password_hash.clone() {
            passwords.push(PasswordKind::Hashed(hash));
        }

        passwords
    }

    /// New user from user, password and database.
    pub fn new(user: &str, password: &str, database: &str) -> Self {
        Self {
            name: user.to_owned(),
            database: database.to_owned(),
            password: Some(password.to_owned()),
            ..Default::default()
        }
    }
}

/// Admin database settings control access to the [admin](https://docs.pgdog.dev/administration/) database which contains real time statistics about internal operations of PgDog.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/admin/
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Admin {
    /// Admin database name.
    ///
    /// _Default:_ `admin`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/admin/#name
    #[serde(default = "Admin::name")]
    pub name: String,
    /// User allowed to connect to the admin database. This user doesn't have to be configured in `users.toml`.
    ///
    /// _Default:_ `admin`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/admin/#user
    #[serde(default = "Admin::user")]
    pub user: String,
    /// Password the user needs to provide when connecting to the admin database. By default, this is randomly generated so the admin database is locked out unless this value is set.
    ///
    /// **Note:** If this value is not set, admin database access will be restricted.
    ///
    /// _Default:_ random
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/admin/#password
    #[serde(default = "Admin::password")]
    #[schemars(default = "Admin::schemars_password_stub")]
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

    /// Generate stable password stub for jsonschema
    fn schemars_password_stub() -> String {
        "_autogenerated_password_".to_string()
    }

    /// The password has been randomly generated.
    pub fn random(&self) -> bool {
        let prefix = "_pgdog_";
        self.password.starts_with(prefix) && self.password.len() == prefix.len() + 12
    }
}

impl Admin {
    pub(crate) fn schemars_default_stub() -> Admin {
        Self {
            name: Self::name(),
            user: Self::user(),
            password: Self::schemars_password_stub(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cutover_swaps_user_database_references() {
        let mut users = Users {
            users: vec![
                User::new("alice", "pass1", "source_db"),
                User::new("bob", "pass2", "source_db"),
                User::new("alice", "pass3", "destination_db"),
                User::new("bob", "pass4", "destination_db"),
            ],
            ..Default::default()
        };

        // cutover swaps user database references
        users.cutover("source_db", "destination_db");

        assert_eq!(users.users.len(), 4);

        // Users that were on source_db should now be on destination_db
        let alice_dest = users
            .users
            .iter()
            .find(|u| u.name == "alice" && u.database == "destination_db")
            .unwrap();
        assert_eq!(alice_dest.password(), "pass1");

        let bob_dest = users
            .users
            .iter()
            .find(|u| u.name == "bob" && u.database == "destination_db")
            .unwrap();
        assert_eq!(bob_dest.password(), "pass2");

        // Users that were on destination_db should now be on source_db
        let alice_source = users
            .users
            .iter()
            .find(|u| u.name == "alice" && u.database == "source_db")
            .unwrap();
        assert_eq!(alice_source.password(), "pass3");

        let bob_source = users
            .users
            .iter()
            .find(|u| u.name == "bob" && u.database == "source_db")
            .unwrap();
        assert_eq!(bob_source.password(), "pass4");
    }

    #[test]
    fn test_user_server_auth_defaults_to_password() {
        let source = r#"
[[users]]
name = "alice"
database = "db"
password = "secret"
"#;

        let users: Users = toml::from_str(source).unwrap();
        let user = users.users.first().unwrap();
        assert_eq!(user.server_auth, ServerAuth::Password);
        assert!(user.server_iam_region.is_none());
    }

    #[test]
    fn test_add_or_replace_adds_new_user() {
        let mut users = Users::default();
        let user = User::new("alice", "pass", "db1");
        users.add_or_replace(user.clone());

        assert_eq!(users.users.len(), 1);
        assert_eq!(users.users[0], user);
    }

    #[test]
    fn test_add_or_replace_replaces_same_name_and_database() {
        let mut users = Users::default();
        users.add_or_replace(User::new("alice", "old_pass", "db1"));
        users.add_or_replace(User::new("alice", "new_pass", "db1"));

        assert_eq!(users.users.len(), 1);
        assert_eq!(users.users[0].password(), "new_pass");
    }

    #[test]
    fn test_add_or_replace_keeps_different_database() {
        let mut users = Users::default();
        users.add_or_replace(User::new("alice", "pass1", "db1"));
        users.add_or_replace(User::new("alice", "pass2", "db2"));

        assert_eq!(users.users.len(), 2);
    }

    #[test]
    fn test_add_or_replace_keeps_different_name() {
        let mut users = Users::default();
        users.add_or_replace(User::new("alice", "pass1", "db1"));
        users.add_or_replace(User::new("bob", "pass2", "db1"));

        assert_eq!(users.users.len(), 2);
    }

    #[test]
    fn test_add_or_replace_multiple_replaces() {
        let mut users = Users::default();
        users.add_or_replace(User::new("alice", "pass1", "db1"));
        users.add_or_replace(User::new("alice", "pass2", "db1"));
        users.add_or_replace(User::new("alice", "pass3", "db1"));

        assert_eq!(users.users.len(), 1);
        assert_eq!(users.users[0].password(), "pass3");
    }

    #[test]
    fn test_add_or_replace_preserves_ordering_of_other_users() {
        let mut users = Users::default();
        users.add_or_replace(User::new("alice", "pass1", "db1"));
        users.add_or_replace(User::new("bob", "pass2", "db1"));
        users.add_or_replace(User::new("charlie", "pass3", "db1"));

        // Replace bob — alice stays, charlie stays, new bob appended at end
        users.add_or_replace(User::new("bob", "new_pass", "db1"));

        assert_eq!(users.users.len(), 3);
        assert_eq!(users.users[0].name, "alice");
        assert_eq!(users.users[1].name, "charlie");
        assert_eq!(users.users[2].name, "bob");
        assert_eq!(users.users[2].password(), "new_pass");
    }

    #[test]
    fn test_find_returns_matching_user() {
        let mut users = Users::default();
        let alice = User::new("alice", "pass1", "db1");
        users.add_or_replace(alice.clone());

        let needle = User::new("alice", "", "db1");
        let found = users.find(&needle).unwrap();
        assert_eq!(found, alice);
    }

    #[test]
    fn test_find_returns_none_when_no_match() {
        let users = Users::default();
        let needle = User::new("alice", "", "db1");
        assert!(users.find(&needle).is_none());
    }

    #[test]
    fn test_find_matches_on_name_and_database() {
        let mut users = Users::default();
        users.add_or_replace(User::new("alice", "pass1", "db1"));
        users.add_or_replace(User::new("alice", "pass2", "db2"));

        let needle = User::new("alice", "", "db2");
        let found = users.find(&needle).unwrap();
        assert_eq!(found.password(), "pass2");

        let needle = User::new("bob", "", "db1");
        assert!(users.find(&needle).is_none());
    }

    #[test]
    fn test_user_server_auth_rds_iam_with_region() {
        let source = r#"
[[users]]
name = "alice"
database = "db"
password = "secret"
server_auth = "rds_iam"
server_iam_region = "us-east-1"
"#;

        let users: Users = toml::from_str(source).unwrap();
        let user = users.users.first().unwrap();
        assert_eq!(user.server_auth, ServerAuth::RdsIam);
        assert_eq!(user.server_iam_region.as_deref(), Some("us-east-1"));
    }
}
