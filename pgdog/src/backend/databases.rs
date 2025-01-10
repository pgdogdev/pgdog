//! Databases behind pgDog.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;

use crate::{
    backend::pool::PoolConfig,
    config::{ConfigAndUsers, Role},
    net::messages::BackendKeyData,
};

use super::{
    pool::{Address, Config},
    Cluster, Error,
};

static DATABASES: Lazy<ArcSwap<Databases>> =
    Lazy::new(|| ArcSwap::from_pointee(Databases::default()));

/// Get databases handle.
///
/// This allows to access any database proxied by pgDog.
pub fn databases() -> Arc<Databases> {
    DATABASES.load().clone()
}

/// Replace databases pooler-wide.
pub fn replace_databases(new_databases: Databases) {
    databases().shutdown();
    DATABASES.store(Arc::new(new_databases));
}

/// Re-create all connections.
pub fn reconnect() {
    replace_databases(databases().duplicate());
}

/// Database/user pair that identifies a database cluster pool.
#[derive(Debug, PartialEq, Hash, Eq, Clone)]
pub struct User {
    /// User name.
    pub user: String,
    /// Database name.
    pub database: String,
}

impl std::fmt::Display for User {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.user, self.database)
    }
}

/// Convert to a database/user pair.
pub trait ToUser {
    /// Perform the conversion.
    fn to_user(&self) -> User;
}

impl ToUser for (&str, &str) {
    fn to_user(&self) -> User {
        User {
            user: self.0.to_string(),
            database: self.1.to_string(),
        }
    }
}

impl ToUser for (&str, Option<&str>) {
    fn to_user(&self) -> User {
        User {
            user: self.0.to_string(),
            database: self.1.map_or(self.0.to_string(), |d| d.to_string()),
        }
    }
}

/// Databases.
#[derive(Default)]
pub struct Databases {
    databases: HashMap<User, Cluster>,
}

impl Databases {
    /// Get a cluster for the user/database pair if it's configured.
    pub fn cluster(&self, user: impl ToUser) -> Result<Cluster, Error> {
        let user = user.to_user();
        if let Some(cluster) = self.databases.get(&user) {
            Ok(cluster.clone())
        } else {
            Err(Error::NoDatabase(user.clone()))
        }
    }

    /// Get all clusters and databases.
    pub fn all(&self) -> &HashMap<User, Cluster> {
        &self.databases
    }

    /// Cancel a query running on one of the databases proxied by the pooler.
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), Error> {
        for cluster in self.databases.values() {
            cluster.cancel(id).await?;
        }

        Ok(())
    }

    /// Create new identical databases.
    fn duplicate(&self) -> Databases {
        Self {
            databases: self
                .databases
                .iter()
                .map(|(k, v)| (k.clone(), v.duplicate()))
                .collect(),
        }
    }

    /// Shutdown all pools.
    fn shutdown(&self) {
        for cluster in self.all().values() {
            for shard in cluster.shards() {
                for pool in shard.pools() {
                    pool.shutdown();
                }
            }
        }
    }
}

/// Load databases from config.
pub fn from_config(config: &ConfigAndUsers) -> Arc<Databases> {
    let mut databases = HashMap::new();
    let config_databases = config.config.databases();
    let general = &config.config.general;

    for user in &config.users.users {
        if let Some(user_databases) = config_databases.get(&user.database) {
            let primary = user_databases
                .iter()
                .find(|d| d.role == Role::Primary)
                .map(|primary| PoolConfig {
                    address: Address::new(primary, user),
                    config: Config::new(general, primary, user),
                });
            let replicas = user_databases
                .iter()
                .filter(|d| d.role == Role::Replica)
                .map(|replica| PoolConfig {
                    address: Address::new(replica, user),
                    config: Config::new(general, replica, user),
                })
                .collect::<Vec<_>>();

            databases.insert(
                User {
                    user: user.name.clone(),
                    database: user.database.clone(),
                },
                Cluster::new(
                    &user.database,
                    &[(primary, &replicas)],
                    general.load_balancing_strategy,
                    &user.password,
                ),
            );
        }
    }

    let databases = Arc::new(Databases { databases });

    DATABASES.store(databases.clone());

    databases
}
