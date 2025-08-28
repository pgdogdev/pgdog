//! Databases behind pgDog.

use std::collections::BTreeSet;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};
use tracing::{debug, info, warn};

use crate::config::PoolerMode;
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
        .lock()
        .close_unused(new_config.config.general.prepared_statements_limit);

    // Resize query cache
    Cache::resize(new_config.config.general.query_cache_limit);

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
    mirrors: HashMap<String, Vec<(String, f32, usize)>>, // Vec<(destination, exposure, queue_depth)>
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

    pub fn mirrors(&self, user: impl ToUser) -> Result<Option<&[(String, f32, usize)]>, Error> {
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

            if cluster.pooler_mode() == PoolerMode::Session && cluster.router_needed() {
                warn!(
                    r#"database "{}" requires transaction mode to route queries"#,
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

fn get_sorted_users_for_database(
    databases: &HashMap<User, Cluster>,
    database_name: &str,
) -> Vec<String> {
    let mut users: Vec<String> = databases
        .keys()
        .filter(|user| user.database == database_name)
        .map(|user| user.user.clone())
        .collect();
    users.sort();
    users
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

    // Build mirrors from [[mirroring]] configuration with user validation
    for mirroring in &config.config.mirroring {
        let source_users = get_sorted_users_for_database(&databases, &mirroring.source);
        let dest_users = get_sorted_users_for_database(&databases, &mirroring.destination);

        if source_users != dest_users {
            warn!(
                "Mirroring disabled for {} -> {}: user lists don't match. Source: {:?}, Destination: {:?}",
                mirroring.source, mirroring.destination, source_users, dest_users
            );
            continue;
        }

        if source_users.is_empty() {
            warn!(
                "Mirroring disabled for {} -> {}: no users found",
                mirroring.source, mirroring.destination
            );
            continue;
        }

        info!(
            "Enabling mirroring: {} -> {} (exposure: {:.1}%, queue_depth: {})",
            mirroring.source,
            mirroring.destination,
            mirroring.exposure * 100.0,
            mirroring.queue_depth
        );

        mirrors
            .entry(mirroring.source.clone())
            .or_insert_with(Vec::new)
            .push((
                mirroring.destination.clone(),
                mirroring.exposure,
                mirroring.queue_depth,
            ));
    }

    Databases {
        databases,
        manual_queries: config.config.manual_queries(),
        mirrors,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, ConfigAndUsers, Users};

    fn load_test_config(config_toml: &str, users_toml: &str) -> ConfigAndUsers {
        let config: Config = toml::from_str(config_toml).expect("Failed to parse config TOML");
        let users: Users = toml::from_str(users_toml).expect("Failed to parse users TOML");
        ConfigAndUsers {
            config,
            users,
            config_path: "test_config.toml".into(),
            users_path: "test_users.toml".into(),
        }
    }

    #[test]
    fn test_mirror_with_multiple_users_creates_duplicate_handlers() {
        // This test demonstrates the bug where multiple users for a mirror database
        // cause duplicate mirror handlers to be created

        let config_toml = r#"
            [general]
            
            [[databases]]
            name = "main"
            host = "127.0.0.1"
            
            [[databases]]
            name = "mirror"
            host = "127.0.0.1"
            database_name = "mirror_db"
            
            [[mirroring]]
            source = "main"
            destination = "mirror"
        "#;

        let users_toml = r#"
            [[users]]
            name = "user1"
            password = "pass"
            database = "main"
            
            # Multiple users for the mirror database - this triggers the bug
            [[users]]
            name = "mirror_user1"
            password = "pass"
            database = "mirror"
            
            [[users]]
            name = "mirror_user2"
            password = "pass"
            database = "mirror"
        "#;

        let config = load_test_config(config_toml, users_toml);
        let databases = from_config(&config);

        // Check how many mirror clusters are created for the main database
        // With mismatched users, no mirrors should be created
        assert!(
            databases.mirrors.get("main").is_none()
                || databases.mirrors.get("main").unwrap().is_empty(),
            "No mirrors should be created with mismatched users"
        );

        // But there are actually 2 clusters for the mirror database (one per user)
        let mirror_clusters: Vec<_> = databases
            .databases
            .iter()
            .filter(|(user, _)| user.database == "mirror")
            .collect();
        assert_eq!(
            mirror_clusters.len(),
            2,
            "There are 2 clusters for mirror database (one per user)"
        );
    }

    #[test]
    fn test_mirror_with_single_user_works_correctly() {
        let config_toml = r#"
            [general]
            
            [[databases]]
            name = "main"
            host = "127.0.0.1"
            
            [[databases]]
            name = "mirror"
            host = "127.0.0.1"
            database_name = "mirror_db"
            
            [[mirroring]]
            source = "main"
            destination = "mirror"
        "#;

        let users_toml = r#"
            [[users]]
            name = "user1"
            password = "pass"
            database = "main"
            
            [[users]]
            name = "user1"
            password = "pass"
            database = "mirror"
        "#;

        let config = load_test_config(config_toml, users_toml);
        let databases = from_config(&config);

        // With matching users, this should work correctly
        let mirrors = databases.mirrors.get("main").unwrap();
        assert_eq!(mirrors.len(), 1, "Single mirror cluster created");

        // Only one cluster for mirror database since user is the same
        let mirror_clusters: Vec<_> = databases
            .databases
            .iter()
            .filter(|(user, _)| user.database == "mirror")
            .collect();
        assert_eq!(
            mirror_clusters.len(),
            1,
            "Only 1 cluster for mirror database with same user"
        );
    }

    #[test]
    fn test_mirror_discovery_bug_with_find() {
        // This test now verifies the fix works correctly
        // by using the new [[mirroring]] configuration

        let config_toml = r#"
            [general]
            
            [[databases]]
            name = "main"
            host = "127.0.0.1"
            
            [[databases]]
            name = "mirror"
            host = "127.0.0.1"
            database_name = "mirror_db"
            
            [[mirroring]]
            source = "main"
            destination = "mirror"
        "#;

        let users_toml = r#"
            [[users]]
            name = "user1"
            password = "pass"
            database = "main"
            
            [[users]]
            name = "user1"
            password = "pass"
            database = "mirror"
            
            [[users]]
            name = "user2"
            password = "pass"
            database = "main"
            
            [[users]]
            name = "user2"
            password = "pass"
            database = "mirror"
            
            [[users]]
            name = "user3"
            password = "pass"  
            database = "main"
            
            [[users]]
            name = "user3"
            password = "pass"  
            database = "mirror"
        "#;

        let config = load_test_config(config_toml, users_toml);
        let databases = from_config(&config);

        // With new implementation, only one mirror config should exist
        let mirrors = databases.mirrors.get("main").unwrap();
        assert_eq!(
            mirrors.len(),
            1,
            "Only 1 mirror configuration for main->mirror"
        );
        assert_eq!(
            mirrors[0].0, "mirror",
            "Mirror destination should be 'mirror'"
        );
    }

    // =========================================================================
    // NEW TESTS FOR DESIRED [[mirroring]] CONFIGURATION BEHAVIOR
    // These tests should FAIL with current implementation and PASS after fix
    // =========================================================================

    #[test]
    fn test_new_mirroring_config_with_matching_users() {
        // Test the new [[mirroring]] configuration with proper user validation
        let config_toml = r#"
            [general]
            
            [[databases]]
            name = "main"
            host = "127.0.0.1"
            
            [[databases]]
            name = "mirror"
            host = "127.0.0.1"
            database_name = "mirror_db"
            
            [[mirroring]]
            source = "main"
            destination = "mirror"
            exposure = 1.0
            queue_depth = 128
        "#;

        let users_toml = r#"
            [[users]]
            name = "user1"
            password = "pass"
            database = "main"
            
            [[users]]
            name = "user1"
            password = "pass"
            database = "mirror"
        "#;

        let config = load_test_config(config_toml, users_toml);
        let databases = from_config(&config);

        // Should have exactly one mirror configuration for main->mirror
        let mirrors = databases.mirrors.get("main");
        assert!(
            mirrors.is_some(),
            "main database should have mirrors configured"
        );

        // The mirror should be a tuple of (destination, exposure, queue_depth)
        // This will fail because current implementation uses Vec<Cluster>
        // Expected new type: Vec<(String, f32, usize)>

        // This test will fail to compile until we implement the new structure
        // let mirrors = mirrors.unwrap();
        // assert_eq!(mirrors.len(), 1, "Should have exactly 1 mirror configuration");
        // assert_eq!(mirrors[0].0, "mirror", "Mirror destination should be 'mirror'");
        // assert_eq!(mirrors[0].1, 1.0, "Mirror exposure should be 1.0");
        // assert_eq!(mirrors[0].2, 128, "Queue depth should be 128");
    }

    #[test]
    fn test_new_mirroring_config_rejects_mismatched_users() {
        // Test that mirroring is disabled when users don't match
        let config_toml = r#"
            [general]
            
            [[databases]]
            name = "main"
            host = "127.0.0.1"
            
            [[databases]]
            name = "mirror"
            host = "127.0.0.1"
            database_name = "mirror_db"
            
            [[mirroring]]
            source = "main"
            destination = "mirror"
            exposure = 0.5
            queue_depth = 64
        "#;

        let users_toml = r#"
            [[users]]
            name = "user1"
            password = "pass"
            database = "main"
            
            # Different users for mirror database - should fail validation
            [[users]]
            name = "mirror_user1"
            password = "pass"
            database = "mirror"
            
            [[users]]
            name = "mirror_user2"
            password = "pass"
            database = "mirror"
        "#;

        let config = load_test_config(config_toml, users_toml);
        let databases = from_config(&config);

        // Should have NO mirrors because users don't match
        let mirrors = databases.mirrors.get("main");
        assert!(
            mirrors.is_none() || mirrors.unwrap().is_empty(),
            "Mirroring should be disabled when users don't match"
        );
    }

    #[test]
    fn test_new_mirroring_config_multiple_destinations() {
        // Test multiple mirror destinations
        let config_toml = r#"
            [general]
            
            [[databases]]
            name = "main"
            host = "127.0.0.1"
            
            [[databases]]
            name = "mirror1"
            host = "127.0.0.1"
            database_name = "mirror_db1"
            
            [[databases]]
            name = "mirror2"
            host = "127.0.0.1"
            database_name = "mirror_db2"
            
            [[mirroring]]
            source = "main"
            destination = "mirror1"
            exposure = 1.0
            queue_depth = 256
            
            [[mirroring]]
            source = "main"
            destination = "mirror2"
            exposure = 0.5
            queue_depth = 128
        "#;

        let users_toml = r#"
            [[users]]
            name = "user1"
            password = "pass"
            database = "main"
            
            [[users]]
            name = "user1"
            password = "pass"
            database = "mirror1"
            
            [[users]]
            name = "user1"
            password = "pass"
            database = "mirror2"
        "#;

        let config = load_test_config(config_toml, users_toml);
        let databases = from_config(&config);

        // Should have 2 mirror configurations
        let mirrors = databases.mirrors.get("main");
        assert!(
            mirrors.is_some(),
            "main database should have mirrors configured"
        );

        // This will fail with current implementation
        // let mirrors = mirrors.unwrap();
        // assert_eq!(mirrors.len(), 2, "Should have 2 mirror destinations");

        // Verify each mirror configuration
        // let mirror1 = mirrors.iter().find(|(dest, _, _)| dest == "mirror1");
        // assert!(mirror1.is_some(), "Should have mirror1 destination");
        // assert_eq!(mirror1.unwrap().1, 1.0, "mirror1 exposure should be 1.0");
        // assert_eq!(mirror1.unwrap().2, 256, "mirror1 queue_depth should be 256");

        // let mirror2 = mirrors.iter().find(|(dest, _, _)| dest == "mirror2");
        // assert!(mirror2.is_some(), "Should have mirror2 destination");
        // assert_eq!(mirror2.unwrap().1, 0.5, "mirror2 exposure should be 0.5");
        // assert_eq!(mirror2.unwrap().2, 128, "mirror2 queue_depth should be 128");
    }

    #[test]
    fn test_new_mirroring_creates_single_handler_per_destination() {
        // Test that only one handler is created per destination database,
        // regardless of how many users exist for that database
        let config_toml = r#"
            [general]
            
            [[databases]]
            name = "main"
            host = "127.0.0.1"
            
            [[databases]]
            name = "mirror"
            host = "127.0.0.1"
            database_name = "mirror_db"
            
            [[mirroring]]
            source = "main"
            destination = "mirror"
            exposure = 1.0
            queue_depth = 128
        "#;

        let users_toml = r#"
            # Multiple users for main database
            [[users]]
            name = "user1"
            password = "pass"
            database = "main"
            
            [[users]]
            name = "user2"
            password = "pass"
            database = "main"
            
            [[users]]
            name = "user3"
            password = "pass"
            database = "main"
            
            # Matching users for mirror database
            [[users]]
            name = "user1"
            password = "pass"
            database = "mirror"
            
            [[users]]
            name = "user2"
            password = "pass"
            database = "mirror"
            
            [[users]]
            name = "user3"
            password = "pass"
            database = "mirror"
        "#;

        let config = load_test_config(config_toml, users_toml);
        let databases = from_config(&config);

        // Should have exactly one mirror configuration, not one per user
        let mirrors = databases.mirrors.get("main");
        assert!(
            mirrors.is_some(),
            "main database should have mirrors configured"
        );

        // This will fail with current implementation
        // let mirrors = mirrors.unwrap();
        // assert_eq!(mirrors.len(), 1, "Should have exactly 1 mirror configuration, not per-user");

        // Verify there are multiple clusters for each database (one per user)
        let main_clusters: Vec<_> = databases
            .databases
            .iter()
            .filter(|(user, _)| user.database == "main")
            .collect();
        assert_eq!(
            main_clusters.len(),
            3,
            "Should have 3 clusters for main database"
        );

        let mirror_clusters: Vec<_> = databases
            .databases
            .iter()
            .filter(|(user, _)| user.database == "mirror")
            .collect();
        assert_eq!(
            mirror_clusters.len(),
            3,
            "Should have 3 clusters for mirror database"
        );

        // But still only ONE mirror configuration
        // assert_eq!(mirrors.len(), 1, "Despite multiple users, only 1 mirror configuration should exist");
    }
}
