//! Databases behind pgDog.

use std::collections::BTreeSet;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};
use tracing::{info, warn};

use crate::{
    backend::pool::PoolConfig,
    config::{config, load, ConfigAndUsers, ManualQuery, Role},
    net::messages::BackendKeyData,
};

use super::{
    pool::{Address, ClusterConfig, Config},
    reload_notify,
    replication::ReplicationConfig,
    Cluster, ClusterShardConfig, Error, ShardedTables,
};

static DATABASES: Lazy<ArcSwap<Databases>> =
    Lazy::new(|| ArcSwap::from_pointee(Databases::default()));
static LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Sync databases during modification.
pub fn lock() -> MutexGuard<'static, RawMutex, ()> {
    LOCK.lock()
}

/// Get databases handle.
///
/// This allows to access any database proxied by pgDog.
pub fn databases() -> Arc<Databases> {
    DATABASES.load().clone()
}

/// Replace databases pooler-wide.
pub fn replace_databases(new_databases: Databases, reload: bool) {
    // Order of operations is important
    // to ensure zero downtime for clients.
    let old_databases = databases();
    let new_databases = Arc::new(new_databases);
    reload_notify::started();
    if reload {
        // Move whatever connections we can over to new pools.
        old_databases.move_conns_to(&new_databases);
    }
    new_databases.launch();
    DATABASES.store(new_databases);
    old_databases.shutdown();
    reload_notify::done();
}

/// Re-create all connections.
pub fn reconnect() {
    replace_databases(databases().duplicate(), false);
}

/// Initialize the databases for the first time.
pub fn init() {
    let config = config();
    replace_databases(from_config(&config), false);
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

    replace_databases(databases, true);

    Ok(())
}

/// Add new user to pool.
pub(crate) fn add(mut user: crate::config::User) {
    let config = config();
    for existing in &config.users.users {
        if existing.name == user.name && existing.database == user.database {
            let mut existing = existing.clone();
            existing.password = user.password.clone();
            user = existing;
        }
    }
    let pool = new_pool(&user, &config.config);
    if let Some((user, cluster)) = pool {
        let _lock = LOCK.lock();
        let databases = (*databases()).clone();
        let (added, databases) = databases.add(user, cluster);
        if added {
            // Launch the new pool (idempotent).
            databases.launch();
            // Don't use replace_databases because Arc refers to the same DBs,
            // and we'll shut them down.
            DATABASES.store(Arc::new(databases));
        }
    }
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
#[derive(Default, Clone)]
pub struct Databases {
    databases: HashMap<User, Cluster>,
    manual_queries: HashMap<String, ManualQuery>,
    mirrors: HashMap<String, Vec<Cluster>>,
}

impl Databases {
    /// Add new connection pools to the databases.
    fn add(mut self, user: User, cluster: Cluster) -> (bool, Databases) {
        match self.databases.entry(user) {
            Entry::Vacant(e) => {
                e.insert(cluster);
                (true, self)
            }
            Entry::Occupied(mut e) => {
                if e.get().password().is_empty() {
                    e.insert(cluster);
                    (true, self)
                } else {
                    (false, self)
                }
            }
        }
    }

    /// Check if a cluster exists, quickly.
    pub fn exists(&self, user: impl ToUser) -> bool {
        if let Some(cluster) = self.databases.get(&user.to_user()) {
            !cluster.password().is_empty()
        } else {
            false
        }
    }

    /// Get a cluster for the user/database pair if it's configured.
    pub fn cluster(&self, user: impl ToUser) -> Result<Cluster, Error> {
        let user = user.to_user();
        if let Some(cluster) = self.databases.get(&user) {
            Ok(cluster.clone())
        } else {
            Err(Error::NoDatabase(user.clone()))
        }
    }

    pub fn mirrors(&self, user: impl ToUser) -> Result<Option<&[Cluster]>, Error> {
        let user = user.to_user();
        if let Some(cluster) = self.databases.get(&user) {
            let name = cluster.name();
            Ok(self.mirrors.get(name).map(|m| m.as_slice()))
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

    /// Manual queries collection, keyed by query fingerprint.
    pub fn manual_queries(&self) -> &HashMap<String, ManualQuery> {
        &self.manual_queries
    }

    /// Move all connections we can from old databases config to new
    /// databases config.
    pub(crate) fn move_conns_to(&self, destination: &Databases) -> usize {
        let mut moved = 0;
        for (user, cluster) in &self.databases {
            let dest = destination.databases.get(user);

            if let Some(dest) = dest {
                if cluster.can_move_conns_to(dest) {
                    cluster.move_conns_to(dest);
                    moved += 1;
                }
            }
        }

        moved
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
            mirrors: self.mirrors.clone(),
        }
    }

    /// Shutdown all pools.
    fn shutdown(&self) {
        for cluster in self.all().values() {
            cluster.shutdown();
        }
    }

    /// Launch all pools.
    fn launch(&self) {
        let mirrors = self.all().values().filter(|c| c.mirror_of().is_some());
        let normal = self.all().values().filter(|c| c.mirror_of().is_none());
        for cluster in mirrors.chain(normal) {
            cluster.launch();
            if let Some(mirror_of) = cluster.mirror_of() {
                info!(
                    r#"enabling mirroring of database "{}" into "{}""#,
                    mirror_of,
                    cluster.name(),
                );
            }
        }
    }
}

pub(crate) fn new_pool(
    user: &crate::config::User,
    config: &crate::config::Config,
) -> Option<(User, Cluster)> {
    let sharded_tables = config.sharded_tables();
    let omnisharded_tables = config.omnisharded_tables();
    let general = &config.general;
    let databases = config.databases();
    let shards = databases.get(&user.database);
    let mut mirrors_of = BTreeSet::new();

    if let Some(shards) = shards {
        let mut shard_configs = vec![];
        for user_databases in shards {
            let primary = user_databases
                .iter()
                .find(|d| d.role == Role::Primary)
                .map(|primary| {
                    mirrors_of.insert(primary.mirror_of.clone());
                    PoolConfig {
                        address: Address::new(primary, user),
                        config: Config::new(general, primary, user),
                    }
                });
            let replicas = user_databases
                .iter()
                .filter(|d| d.role == Role::Replica)
                .map(|replica| {
                    mirrors_of.insert(replica.mirror_of.clone());
                    PoolConfig {
                        address: Address::new(replica, user),
                        config: Config::new(general, replica, user),
                    }
                })
                .collect::<Vec<_>>();

            shard_configs.push(ClusterShardConfig { primary, replicas });
        }

        let sharded_tables = sharded_tables
            .get(&user.database)
            .cloned()
            .unwrap_or(vec![]);
        let omnisharded_tables = omnisharded_tables
            .get(&user.database)
            .cloned()
            .unwrap_or(vec![]);
        let sharded_tables =
            ShardedTables::new(sharded_tables, omnisharded_tables, general.dry_run);
        // Make sure all nodes in the cluster agree they are mirroring the same cluster.
        let mirror_of = match mirrors_of.len() {
            0 => None,
            1 => mirrors_of
                .first()
                .and_then(|s| s.as_ref().map(|s| s.as_str())),
            _ => {
                warn!(
                    "database \"{}\" has different \"mirror_of\" settings, disabling mirroring",
                    user.database
                );
                None
            }
        };

        let cluster_config = ClusterConfig::new(
            general,
            user,
            &shard_configs,
            sharded_tables,
            mirror_of,
            config.multi_tenant(),
        );

        Some((
            User {
                user: user.name.clone(),
                database: user.database.clone(),
            },
            Cluster::new(cluster_config),
        ))
    } else {
        None
    }
}

/// Load databases from config.
pub fn from_config(config: &ConfigAndUsers) -> Databases {
    let mut databases = HashMap::new();

    for user in &config.users.users {
        if let Some((user, cluster)) = new_pool(user, &config.config) {
            databases.insert(user, cluster);
        }
    }

    let mut mirrors = HashMap::new();

    for cluster in databases.values() {
        let mirror_clusters = databases
            .iter()
            .find(|(_, c)| c.mirror_of() == Some(cluster.name()))
            .map(|(_, c)| c.clone())
            .into_iter()
            .collect::<Vec<_>>();
        mirrors.insert(cluster.name().to_owned(), mirror_clusters);
    }

    Databases {
        databases,
        manual_queries: config.config.manual_queries(),
        mirrors,
    }
}
