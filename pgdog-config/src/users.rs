use serde::{Deserialize, Serialize};
use std::env;
use tracing::warn;

use super::core::Config;
use super::pooling::PoolerMode;
use crate::util::random_string;
use schemars::JsonSchema;

/// pgDog plugin.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Plugin {
    /// Plugin name.
    pub name: String,
}

/// Users and passwords.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Users {
    pub admin: Option<Admin>,
    /// Users and passwords.
    #[serde(default)]
    pub users: Vec<User>,
}

impl Users {
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
}

/// User allowed to connect to pgDog.
#[derive(
    Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, Ord, PartialOrd, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct User {
    /// User name.
    pub name: String,
    /// Database name, from pgdog.toml.
    #[serde(default)]
    pub database: String,
    /// List of databases the user has access to.
    #[serde(default)]
    pub databases: Vec<String>,
    /// User belongs to all databases
    #[serde(default)]
    pub all_databases: bool,
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
    /// Two-pc.
    pub two_phase_commit: Option<bool>,
    /// Automatic transactions.
    pub two_phase_commit_auto: Option<bool>,
    /// Server lifetime.
    pub server_lifetime: Option<u64>,
}

impl User {
    pub fn password(&self) -> &str {
        if let Some(ref s) = self.password {
            s.as_str()
        } else {
            ""
        }
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

/// Admin database settings.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
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
}
