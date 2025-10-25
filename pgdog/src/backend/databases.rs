//! Databases behind pgDog.

use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};
use tracing::{debug, error, info, warn};

use crate::config::PoolerMode;
use crate::frontend::client::query_engine::two_pc::Manager;
use crate::frontend::router::parser::Cache;
use crate::frontend::router::sharding::Mapping;
use crate::frontend::PreparedStatements;
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

    // Resize query cache
    Cache::resize(config.config.general.query_cache_limit);

    // Start two-pc manager.
    let _monitor = Manager::get();
}

/// Shutdown all databases.
pub fn shutdown() {
    databases().shutdown();
}

/// Re-create pools from config.
pub fn reload() -> Result<(), Error> {
    let old_config = config();
    let new_config = load(&old_config.config_path, &old_config.users_path)?;
    let databases = from_config(&new_config);

    replace_databases(databases, true);

    // Remove any unused prepared statements.
    PreparedStatements::global()
        .write()
        .close_unused(new_config.config.general.prepared_statements_limit);

    // Resize query cache
    Cache::resize(new_config.config.general.query_cache_limit);

    // Reload rate limiter with new limit (or disable if None)
    crate::auth::rate_limit::reload(new_config.config.general.auth_rate_limit);

    Ok(())
}

/// Add new user to pool.
pub(crate) fn add(mut user: crate::config::User) {
    // One user at a time.
    let _lock = lock();

    debug!(
        "adding user \"{}\" for database \"{}\" via auth passthrough",
        user.name, user.database
    );

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
#[derive(Debug, PartialEq, Hash, Eq, Clone, Default)]
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
    mirrors: HashMap<User, Vec<Cluster>>,
    mirror_configs: HashMap<(String, String), crate::config::MirrorConfig>,
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

    /// Get the schema owner for this database.
    pub fn schema_owner(&self, database: &str) -> Result<Cluster, Error> {
        for (user, cluster) in &self.databases {
            if cluster.schema_admin() && user.database == database {
                return Ok(cluster.clone());
            }
        }

        Err(Error::NoSchemaOwner(database.to_owned()))
    }

    pub fn mirrors(&self, user: impl ToUser) -> Result<Option<&[Cluster]>, Error> {
        let user = user.to_user();
        if self.databases.contains_key(&user) {
            Ok(self.mirrors.get(&user).map(|m| m.as_slice()))
        } else {
            Err(Error::NoDatabase(user.clone()))
        }
    }

    /// Get precomputed mirror configuration.
    pub fn mirror_config(
        &self,
        source_db: &str,
        destination_db: &str,
    ) -> Option<&crate::config::MirrorConfig> {
        self.mirror_configs
            .get(&(source_db.to_string(), destination_db.to_string()))
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
            mirror_configs: self.mirror_configs.clone(),
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
        // Launch mirrors first to log mirror relationships
        for (source_user, mirror_clusters) in &self.mirrors {
            if let Some(source_cluster) = self.databases.get(source_user) {
                for mirror_cluster in mirror_clusters {
                    info!(
                        r#"enabling mirroring of database "{}" into "{}""#,
                        source_cluster.name(),
                        mirror_cluster.name(),
                    );
                }
            }
        }

        // Launch all clusters
        for cluster in self.all().values() {
            cluster.launch();

            if cluster.pooler_mode() == PoolerMode::Session && cluster.router_needed() {
                warn!(
                    r#"user "{}" for database "{}" requires transaction mode to route queries"#,
                    cluster.user(),
                    cluster.name()
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
    let sharded_mappings = config.sharded_mappings();
    let general = &config.general;
    let databases = config.databases();
    let shards = databases.get(&user.database);

    if let Some(shards) = shards {
        let mut shard_configs = vec![];
        for user_databases in shards {
            let has_single_replica = user_databases.len() == 1;
            let primary = user_databases
                .iter()
                .find(|d| d.role == Role::Primary)
                .map(|primary| PoolConfig {
                    address: Address::new(primary, user),
                    config: Config::new(general, primary, user, has_single_replica),
                });
            let replicas = user_databases
                .iter()
                .filter(|d| d.role == Role::Replica)
                .map(|replica| PoolConfig {
                    address: Address::new(replica, user),
                    config: Config::new(general, replica, user, has_single_replica),
                })
                .collect::<Vec<_>>();

            shard_configs.push(ClusterShardConfig { primary, replicas });
        }

        let mut sharded_tables = sharded_tables
            .get(&user.database)
            .cloned()
            .unwrap_or(vec![]);

        for sharded_table in &mut sharded_tables {
            let mappings = sharded_mappings.get(&(
                sharded_table.database.clone(),
                sharded_table.column.clone(),
                sharded_table.name.clone(),
            ));

            if let Some(mappings) = mappings {
                sharded_table.mapping = Mapping::new(mappings);

                if let Some(ref mapping) = sharded_table.mapping {
                    if !mapping.valid() {
                        warn!(
                            "sharded table name=\"{}\", column=\"{}\" has overlapping ranges",
                            sharded_table.name.as_ref().unwrap_or(&String::from("")),
                            sharded_table.column
                        );
                    }
                }
            }
        }

        let omnisharded_tables = omnisharded_tables
            .get(&user.database)
            .cloned()
            .unwrap_or(vec![]);
        let sharded_tables = ShardedTables::new(sharded_tables, omnisharded_tables);

        let cluster_config = ClusterConfig::new(
            general,
            user,
            &shard_configs,
            sharded_tables,
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

    // Duplicate schema owner check.
    let mut dupl_schema_owners = HashMap::<String, usize>::new();
    for (user, cluster) in &mut databases {
        if cluster.schema_admin() {
            let entry = dupl_schema_owners.entry(user.database.clone()).or_insert(0);
            *entry += 1;

            if *entry > 1 {
                warn!(
                    r#"database "{}" has duplicate schema owner "{}", ignoring setting"#,
                    user.database, user.user
                );
                cluster.toggle_schema_admin(false);
            }
        }
    }

    let mut mirrors = HashMap::new();

    // Helper function to get users for a database
    let get_database_users = |db_name: &str| -> std::collections::HashSet<&String> {
        databases
            .iter()
            .filter(|(_, cluster)| cluster.name() == db_name)
            .map(|(user, _)| &user.user)
            .collect()
    };

    // Validate mirroring configurations and collect valid ones
    let mut valid_mirrors = std::collections::HashSet::new();

    for mirror_config in &config.config.mirroring {
        let source_users = get_database_users(&mirror_config.source_db);
        let dest_users = get_database_users(&mirror_config.destination_db);

        if !source_users.is_empty() && !dest_users.is_empty() && source_users == dest_users {
            valid_mirrors.insert((
                mirror_config.source_db.clone(),
                mirror_config.destination_db.clone(),
            ));
        } else {
            error!(
                "mirroring disabled from \"{}\" into \"{}\": users don't match",
                mirror_config.source_db, mirror_config.destination_db
            );
        }
    }

    // Build mirrors only for valid configurations
    for (source_user, source_cluster) in databases.iter() {
        let mut mirror_clusters_with_config = vec![];

        // Check if this database is a source in any valid mirroring configuration
        for mirror in &config.config.mirroring {
            if mirror.source_db == source_cluster.name()
                && valid_mirrors
                    .contains(&(mirror.source_db.clone(), mirror.destination_db.clone()))
            {
                // Find the destination cluster for this user
                if let Some((_dest_user, dest_cluster)) =
                    databases.iter().find(|(user, cluster)| {
                        user.user == source_user.user && cluster.name() == mirror.destination_db
                    })
                {
                    mirror_clusters_with_config.push(dest_cluster.clone());
                }
            }
        }

        if !mirror_clusters_with_config.is_empty() {
            mirrors.insert(source_user.clone(), mirror_clusters_with_config);
        }
    }

    // Build precomputed mirror configurations
    let mut mirror_configs = HashMap::new();
    for mirror in &config.config.mirroring {
        if valid_mirrors.contains(&(mirror.source_db.clone(), mirror.destination_db.clone())) {
            let mirror_config = crate::config::MirrorConfig {
                queue_length: mirror
                    .queue_length
                    .unwrap_or(config.config.general.mirror_queue),
                exposure: mirror
                    .exposure
                    .unwrap_or(config.config.general.mirror_exposure),
            };
            mirror_configs.insert(
                (mirror.source_db.clone(), mirror.destination_db.clone()),
                mirror_config,
            );
        }
    }

    Databases {
        databases,
        manual_queries: config.config.manual_queries(),
        mirrors,
        mirror_configs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, ConfigAndUsers, Database, Role};

    #[test]
    fn test_mirror_user_isolation() {
        // Test that each user gets their own mirror cluster
        let mut config = Config::default();

        // Source database and one mirror destination
        config.databases = vec![
            Database {
                name: "db1".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                role: Role::Primary,
                ..Default::default()
            },
            Database {
                name: "db1_mirror".to_string(),
                host: "localhost".to_string(),
                port: 5433,
                role: Role::Primary,
                ..Default::default()
            },
        ];

        // Set up mirroring configuration - one mirror for all users
        config.mirroring = vec![crate::config::Mirroring {
            source_db: "db1".to_string(),
            destination_db: "db1_mirror".to_string(),
            queue_length: None,
            exposure: None,
        }];

        let users = crate::config::Users {
            users: vec![
                crate::config::User {
                    name: "alice".to_string(),
                    database: "db1".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                crate::config::User {
                    name: "bob".to_string(),
                    database: "db1".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                crate::config::User {
                    name: "alice".to_string(),
                    database: "db1_mirror".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                crate::config::User {
                    name: "bob".to_string(),
                    database: "db1_mirror".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
            ],
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
        });

        let alice_mirrors = databases.mirrors(("alice", "db1")).unwrap().unwrap_or(&[]);
        let bob_mirrors = databases.mirrors(("bob", "db1")).unwrap().unwrap_or(&[]);

        // Each user should get their own mirror cluster (but same destination database)
        assert_eq!(alice_mirrors.len(), 1);
        assert_eq!(alice_mirrors[0].user(), "alice");
        assert_eq!(alice_mirrors[0].name(), "db1_mirror");

        assert_eq!(bob_mirrors.len(), 1);
        assert_eq!(bob_mirrors[0].user(), "bob");
        assert_eq!(bob_mirrors[0].name(), "db1_mirror");
    }

    #[test]
    fn test_mirror_user_mismatch_handling() {
        // Test that mirroring is disabled gracefully when users don't match
        let mut config = Config::default();

        // Source database with two users, destination with only one
        config.databases = vec![
            Database {
                name: "source_db".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                role: Role::Primary,
                ..Default::default()
            },
            Database {
                name: "dest_db".to_string(),
                host: "localhost".to_string(),
                port: 5433,
                role: Role::Primary,
                ..Default::default()
            },
        ];

        config.mirroring = vec![crate::config::Mirroring {
            source_db: "source_db".to_string(),
            destination_db: "dest_db".to_string(),
            queue_length: None,
            exposure: None,
        }];

        let users = crate::config::Users {
            users: vec![
                crate::config::User {
                    name: "user1".to_string(),
                    database: "source_db".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                crate::config::User {
                    name: "user2".to_string(),
                    database: "source_db".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                crate::config::User {
                    name: "user1".to_string(),
                    database: "dest_db".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                // Note: user2 missing for dest_db - this should disable mirroring
            ],
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
        });

        // Mirrors should be empty due to user mismatch
        let user1_mirrors = databases.mirrors(("user1", "source_db")).unwrap();
        let user2_mirrors = databases.mirrors(("user2", "source_db")).unwrap();

        assert!(
            user1_mirrors.is_none() || user1_mirrors.unwrap().is_empty(),
            "Expected no mirrors for user1 due to user mismatch"
        );
        assert!(
            user2_mirrors.is_none() || user2_mirrors.unwrap().is_empty(),
            "Expected no mirrors for user2 due to user mismatch"
        );
    }

    #[test]
    fn test_precomputed_mirror_configs() {
        // Test that mirror configs are precomputed correctly during initialization
        let mut config = Config::default();
        config.general.mirror_queue = 100;
        config.general.mirror_exposure = 0.8;

        config.databases = vec![
            Database {
                name: "source_db".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                role: Role::Primary,
                ..Default::default()
            },
            Database {
                name: "dest_db".to_string(),
                host: "localhost".to_string(),
                port: 5433,
                role: Role::Primary,
                ..Default::default()
            },
        ];

        config.mirroring = vec![crate::config::Mirroring {
            source_db: "source_db".to_string(),
            destination_db: "dest_db".to_string(),
            queue_length: Some(256),
            exposure: Some(0.5),
        }];

        let users = crate::config::Users {
            users: vec![
                crate::config::User {
                    name: "user1".to_string(),
                    database: "source_db".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                crate::config::User {
                    name: "user1".to_string(),
                    database: "dest_db".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
            ],
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
        });

        // Verify mirror config exists and has custom values
        let mirror_config = databases.mirror_config("source_db", "dest_db");
        assert!(
            mirror_config.is_some(),
            "Mirror config should be precomputed"
        );
        let config = mirror_config.unwrap();
        assert_eq!(
            config.queue_length, 256,
            "Custom queue length should be used"
        );
        assert_eq!(config.exposure, 0.5, "Custom exposure should be used");

        // Non-existent mirror config should return None
        let no_config = databases.mirror_config("source_db", "non_existent");
        assert!(
            no_config.is_none(),
            "Non-existent mirror config should return None"
        );
    }

    #[test]
    fn test_mirror_config_with_global_defaults() {
        // Test that global defaults are used when mirror-specific values aren't provided
        let mut config = Config::default();
        config.general.mirror_queue = 150;
        config.general.mirror_exposure = 0.9;

        config.databases = vec![
            Database {
                name: "db1".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                role: Role::Primary,
                ..Default::default()
            },
            Database {
                name: "db2".to_string(),
                host: "localhost".to_string(),
                port: 5433,
                role: Role::Primary,
                ..Default::default()
            },
        ];

        // Mirror config without custom values - should use defaults
        config.mirroring = vec![crate::config::Mirroring {
            source_db: "db1".to_string(),
            destination_db: "db2".to_string(),
            queue_length: None,
            exposure: None,
        }];

        let users = crate::config::Users {
            users: vec![
                crate::config::User {
                    name: "user".to_string(),
                    database: "db1".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                crate::config::User {
                    name: "user".to_string(),
                    database: "db2".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
            ],
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
        });

        let mirror_config = databases.mirror_config("db1", "db2");
        assert!(
            mirror_config.is_some(),
            "Mirror config should be precomputed"
        );
        let config = mirror_config.unwrap();
        assert_eq!(
            config.queue_length, 150,
            "Global default queue length should be used"
        );
        assert_eq!(
            config.exposure, 0.9,
            "Global default exposure should be used"
        );
    }

    #[test]
    fn test_mirror_config_partial_overrides() {
        // Test that we can override just queue or just exposure
        let mut config = Config::default();
        config.general.mirror_queue = 100;
        config.general.mirror_exposure = 1.0;

        config.databases = vec![
            Database {
                name: "primary".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                role: Role::Primary,
                ..Default::default()
            },
            Database {
                name: "mirror1".to_string(),
                host: "localhost".to_string(),
                port: 5433,
                role: Role::Primary,
                ..Default::default()
            },
            Database {
                name: "mirror2".to_string(),
                host: "localhost".to_string(),
                port: 5434,
                role: Role::Primary,
                ..Default::default()
            },
        ];

        config.mirroring = vec![
            crate::config::Mirroring {
                source_db: "primary".to_string(),
                destination_db: "mirror1".to_string(),
                queue_length: Some(200), // Override queue only
                exposure: None,
            },
            crate::config::Mirroring {
                source_db: "primary".to_string(),
                destination_db: "mirror2".to_string(),
                queue_length: None,
                exposure: Some(0.25), // Override exposure only
            },
        ];

        let users = crate::config::Users {
            users: vec![
                crate::config::User {
                    name: "user".to_string(),
                    database: "primary".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                crate::config::User {
                    name: "user".to_string(),
                    database: "mirror1".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                crate::config::User {
                    name: "user".to_string(),
                    database: "mirror2".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
            ],
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
        });

        // Check mirror1 config - custom queue, default exposure
        let mirror1_config = databases.mirror_config("primary", "mirror1").unwrap();
        assert_eq!(
            mirror1_config.queue_length, 200,
            "Custom queue length should be used"
        );
        assert_eq!(
            mirror1_config.exposure, 1.0,
            "Default exposure should be used"
        );

        // Check mirror2 config - default queue, custom exposure
        let mirror2_config = databases.mirror_config("primary", "mirror2").unwrap();
        assert_eq!(
            mirror2_config.queue_length, 100,
            "Default queue length should be used"
        );
        assert_eq!(
            mirror2_config.exposure, 0.25,
            "Custom exposure should be used"
        );
    }

    #[test]
    fn test_invalid_mirror_not_precomputed() {
        // Test that invalid mirror configs (user mismatch) are not precomputed
        let mut config = Config::default();

        config.databases = vec![
            Database {
                name: "source".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                role: Role::Primary,
                ..Default::default()
            },
            Database {
                name: "dest".to_string(),
                host: "localhost".to_string(),
                port: 5433,
                role: Role::Primary,
                ..Default::default()
            },
        ];

        config.mirroring = vec![crate::config::Mirroring {
            source_db: "source".to_string(),
            destination_db: "dest".to_string(),
            queue_length: Some(256),
            exposure: Some(0.5),
        }];

        // Create user mismatch - user1 for source, user2 for dest
        let users = crate::config::Users {
            users: vec![
                crate::config::User {
                    name: "user1".to_string(),
                    database: "source".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                crate::config::User {
                    name: "user2".to_string(), // Different user!
                    database: "dest".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
            ],
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
        });

        // Should not have precomputed this invalid config
        let mirror_config = databases.mirror_config("source", "dest");
        assert!(
            mirror_config.is_none(),
            "Invalid mirror config should not be precomputed"
        );
    }

    #[test]
    fn test_mirror_config_no_users() {
        // Test that mirror configs without any users are not precomputed
        let mut config = Config::default();
        config.general.mirror_queue = 100;
        config.general.mirror_exposure = 0.8;

        config.databases = vec![
            Database {
                name: "source_db".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                role: Role::Primary,
                ..Default::default()
            },
            Database {
                name: "dest_db".to_string(),
                host: "localhost".to_string(),
                port: 5433,
                role: Role::Primary,
                ..Default::default()
            },
        ];

        // Configure mirroring
        config.mirroring = vec![crate::config::Mirroring {
            source_db: "source_db".to_string(),
            destination_db: "dest_db".to_string(),
            queue_length: Some(256),
            exposure: Some(0.5),
        }];

        // No users at all
        let users = crate::config::Users { users: vec![] };

        let databases = from_config(&ConfigAndUsers {
            config: config.clone(),
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
        });

        // Mirror config should not be precomputed when there are no users
        let mirror_config = databases.mirror_config("source_db", "dest_db");
        assert!(
            mirror_config.is_none(),
            "Mirror config should not be precomputed when no users exist"
        );

        // Now test with users for only one database
        let users_partial = crate::config::Users {
            users: vec![
                crate::config::User {
                    name: "user1".to_string(),
                    database: "source_db".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                // No user for dest_db!
            ],
        };

        let databases_partial = from_config(&ConfigAndUsers {
            config: config.clone(),
            users: users_partial,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
        });

        // Mirror config should not be precomputed when destination has no users
        let mirror_config_partial = databases_partial.mirror_config("source_db", "dest_db");
        assert!(
            mirror_config_partial.is_none(),
            "Mirror config should not be precomputed when destination has no users"
        );

        // Test the opposite - users only for destination
        let users_dest_only = crate::config::Users {
            users: vec![
                crate::config::User {
                    name: "user1".to_string(),
                    database: "dest_db".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                // No user for source_db!
            ],
        };

        let databases_dest_only = from_config(&ConfigAndUsers {
            config,
            users: users_dest_only,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
        });

        // Mirror config should not be precomputed when source has no users
        let mirror_config_dest_only = databases_dest_only.mirror_config("source_db", "dest_db");
        assert!(
            mirror_config_dest_only.is_none(),
            "Mirror config should not be precomputed when source has no users"
        );
    }
}
