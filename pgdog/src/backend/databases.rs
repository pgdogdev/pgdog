//! Databases behind pgDog.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;

use crate::{
    backend::pool::PoolConfig,
    config::{config, load, ConfigAndUsers, ManualQuery, Role},
    net::messages::BackendKeyData,
};

use super::{
    pool::{Address, Config},
    replication::ReplicationConfig,
    Cluster, Error, ShardedTables,
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
    // Order of operations is important
    // to ensure zero downtime for clients.
    let old_databases = databases();
    let new_databases = Arc::new(new_databases);
    new_databases.launch();
    DATABASES.store(new_databases);
    old_databases.shutdown();
}

/// Re-create all connections.
pub fn reconnect() {
    replace_databases(databases().duplicate());
}

/// Iniitialize the databases for the first time.
pub fn init() {
    let config = config();
    replace_databases(from_config(&config));
}

/// Shutdown all databases.
pub fn shutdown() {
    databases().shutdown();
}

/// Re-create pools from config.
///
/// TODO: Avoid creating new pools if they haven't changed at all
/// or the configuration between the two is compatible.
pub fn reload() -> Result<(), Error> {
    let old_config = config();
    let new_config = load(&old_config.config_path, &old_config.users_path)?;
    let databases = from_config(&new_config);

    replace_databases(databases);

    Ok(())
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
    manual_queries: HashMap<String, ManualQuery>,
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

    /// Get replication configuration for the database.
    pub fn replication(&self, database: &str) -> Option<ReplicationConfig> {
        for (user, cluster) in &self.databases {
            if user.database == database {
                return Some(ReplicationConfig {
                    shards: cluster.shards().len(),
                    sharded_tables: cluster.sharded_tables().into(),
                });
            }
        }

        None
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

    /// Get manual query, if exists.
    pub fn manual_query(&self, fingerprint: &str) -> Option<&ManualQuery> {
        self.manual_queries.get(fingerprint)
    }

    /// Create new identical databases.
    fn duplicate(&self) -> Databases {
        Self {
            databases: self
                .databases
                .iter()
                .map(|(k, v)| (k.clone(), v.duplicate()))
                .collect(),
            manual_queries: self.manual_queries.clone(),
        }
    }

    /// Shutdown all pools.
    fn shutdown(&self) {
        for cluster in self.all().values() {
            for shard in cluster.shards() {
                shard.shutdown();
            }
        }
    }

    /// Launch all pools.
    fn launch(&self) {
        for cluster in self.all().values() {
            for shard in cluster.shards() {
                shard.launch();
            }
        }
    }
}

/// Load databases from config.
pub fn from_config(config: &ConfigAndUsers) -> Databases {
    let mut databases = HashMap::new();
    let config_databases = config.config.databases();
    let sharded_tables = config.config.sharded_tables();
    let general = &config.config.general;

    for user in &config.users.users {
        if let Some(shards) = config_databases.get(&user.database) {
            let mut shard_configs = vec![];
            for user_databases in shards {
                let primary =
                    user_databases
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
                shard_configs.push((primary, replicas));
            }

            let sharded_tables = sharded_tables
                .get(&user.database)
                .cloned()
                .unwrap_or(vec![]);
            let sharded_tables = ShardedTables::new(sharded_tables);
            databases.insert(
                User {
                    user: user.name.clone(),
                    database: user.database.clone(),
                },
                Cluster::new(
                    &user.database,
                    &shard_configs,
                    general.load_balancing_strategy,
                    &user.password,
                    user.pooler_mode.unwrap_or(general.pooler_mode),
                    sharded_tables,
                    user.replication_sharding.clone(),
                ),
            );
        }
    }

    Databases {
        databases,
        manual_queries: config.config.manual_queries(),
    }
}
