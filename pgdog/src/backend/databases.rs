//! Databases behind pgDog.

use arc_swap::ArcSwap;
use futures::future::{join_all, try_join_all};
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};
use pgdog_config::pool::ShardNodes;
use pgdog_config::users::PasswordKind;
use pgdog_config::util::normalize_identifier;
use pgdog_config::{
    EnumeratedDatabase, QueryParser, ShardedMappingConfig, ShardedMappingKey, ShardedMappingKeyRef,
    ShardedMappingKindDeprecated, ShardedMappingList, ShardedMappingRange, ShardedTableConfig,
};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tracing::{debug, error, info, warn};

use crate::auth::AuthResult;
use crate::backend::passthrough::{Attempt, throttle};
use crate::backend::replication::ShardedSchemas;
use crate::backend::schema::SchemaCache;
use crate::config::PoolerMode;
use crate::frontend::PreparedStatements;
use crate::frontend::client::query_engine::two_pc::Manager;
use crate::frontend::router::parser::Cache;
use crate::frontend::router::sharding::{Mapping, ShardedTable};
use crate::{
    backend::pool::PoolConfig,
    config::{
        ConfigAndUsers, Database as ConfigDatabase, Role, ShardedMappingDeprecated,
        User as ConfigUser, config, load, set,
    },
    net::{messages::FrontendPid, tls},
    util::safe_timeout,
};

use super::{
    Cluster, ClusterShardConfig, ConnectReason, Error, Server, ServerOptions, ShardedTables,
    pool::{Address, ClusterConfig},
    reload_notify,
};

static DATABASES: Lazy<ArcSwap<Databases>> =
    Lazy::new(|| ArcSwap::from_pointee(Databases::default()));
static LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Sync databases during modification.
pub(crate) fn lock() -> MutexGuard<'static, RawMutex, ()> {
    LOCK.lock()
}

/// Get databases handle.
///
/// This allows to access any database proxied by pgDog.
pub(crate) fn databases() -> Arc<Databases> {
    DATABASES.load().clone()
}

/// Replace databases pooler-wide.
pub(crate) fn replace_databases(new_databases: Databases, reload: bool) -> Result<(), Error> {
    // Order of operations is important
    // to ensure zero downtime for clients.
    //
    // 1. Prevent concurrent reloads. The guard restores the ready flag and
    //    wakes waiters on drop, even if a step below errors out.
    let _guard = reload_notify::started();

    // 2. Move connections from old databases into new ones.
    let old_databases = databases();
    let new_databases = Arc::new(new_databases);
    if reload {
        // Move whatever connections we can over to new pools.
        old_databases.move_conns_to(&new_databases)?;
    }
    // 3. Launch new databases first.
    new_databases.launch();
    DATABASES.store(new_databases);
    // 4. Shutdown all databases.
    old_databases.shutdown();

    super::reload_signal::notify();

    Ok(())
}

/// Re-create all connections.
pub(crate) fn reconnect() -> Result<(), Error> {
    let config = config();
    let databases = from_config(&config);
    replace_databases(databases, false)?;
    Ok(())
}

/// Re-create databases from existing config,
/// preserving connections.
pub(crate) fn reload_from_existing() -> Result<(), Error> {
    let _lock = lock();
    let config = config();
    let databases = from_config(&config);
    replace_databases(databases, true)?;
    Ok(())
}

/// Initialize the databases for the first time.
pub(crate) fn init() -> Result<(), Error> {
    let config = config();
    replace_databases(from_config(&config), false)?;

    // Resize query cache
    Cache::resize(config.config.general.query_cache_limit);

    // Start two-pc manager.
    let _monitor = Manager::get();

    Ok(())
}

/// Shutdown all databases.
pub(crate) fn shutdown() {
    databases().shutdown();
}

/// Cancel all queries running on a database.
pub(crate) async fn cancel_all(database: &str) -> Result<(), Error> {
    let clusters: Vec<_> = databases()
        .all()
        .iter()
        .filter(|(user, _)| user.database == database)
        .map(|(_, cluster)| cluster.clone())
        .collect();

    try_join_all(clusters.iter().map(|cluster| cluster.cancel_all())).await?;

    Ok(())
}

/// Terminates all active connections on all `Pool`s.
pub(crate) fn terminate_active_connections() {
    databases()
        .all()
        .values()
        .for_each(Cluster::terminate_active_connections);
}

/// Re-create pools from config.
pub(crate) fn reload(force: bool) -> Result<(), Error> {
    if force {
        info!("force reloading configuration");
    } else {
        info!("reloading configuration");
    }

    // Load config from disk.
    let old_config = config();
    let new_config = load(&old_config.config_path, &old_config.users_path)?;
    let databases = from_config(&new_config);

    // Terminate after checking config for validity.
    if force {
        terminate_active_connections();
    }

    // Replace databases.
    replace_databases(databases, true)?;

    // Reload TLS connectors.
    tls::reload()?;

    // Remove any unused prepared statements.
    PreparedStatements::global()
        .write()
        .close_unused(new_config.config.general.prepared_statements_limit);

    // Resize query cache.
    Cache::resize(new_config.config.general.query_cache_limit);

    Ok(())
}

/// What passthrough authentication should do with a client-supplied credential.
#[derive(Debug, PartialEq)]
enum PassthroughAction {
    /// Credential matches the stored password; the client is authenticated.
    Match,
    /// Credential must be stored: the user is new, has no password yet, or a
    /// password change is permitted by the configuration.
    Store,
    /// Credential is rejected.
    Deny(AuthResult),
}

fn passthrough_action(user: &ConfigUser, config: &ConfigAndUsers) -> PassthroughAction {
    let Some(existing) = config.users.find(user) else {
        return PassthroughAction::Store;
    };

    if existing.password.is_none() {
        PassthroughAction::Store
    } else if existing
        .password
        .as_deref()
        .zip(user.password.as_deref())
        .is_some_and(|(stored, provided)| {
            crate::util::constant_time_eq(stored.as_bytes(), provided.as_bytes())
        })
    {
        PassthroughAction::Match
    } else if config.config.general.passthrough_auth.allows_change() {
        PassthroughAction::Store
    } else {
        PassthroughAction::Deny(AuthResult::NoPassthroughPasswordChange)
    }
}

/// The entry passthrough authentication stores for a client credential.
///
/// A user configured in `users.toml` keeps its settings (pool size, server
/// credentials, ...) and only learns the password; an unknown user is stored
/// as discovered. Both are marked so the credential can be evicted if the
/// server rejects it.
fn passthrough_entry(user: ConfigUser, config: &ConfigAndUsers) -> ConfigUser {
    match config.users.find(&user) {
        Some(mut existing) => {
            existing.password = user.password;
            existing.password_from_passthrough = true;
            existing
        }
        None => ConfigUser {
            password_from_passthrough: true,
            created_by_passthrough: true,
            ..user
        },
    }
}

/// Result of verifying a passthrough credential against the server.
enum Verification {
    Ok,
    BadPassword,
    NoDatabase,
    Failed,
}

/// One database entry per shard to verify a credential against: the shard's
/// primary, or its first entry when no primary is configured.
fn verification_targets<'a>(
    entry: &ConfigUser,
    config: &'a ConfigAndUsers,
) -> Vec<(usize, &'a ConfigDatabase)> {
    let mut targets: Vec<(usize, &ConfigDatabase)> = Vec::new();

    for (number, database) in config.config.databases.iter().enumerate() {
        if database.name != entry.database {
            continue;
        }

        match targets
            .iter_mut()
            .find(|(_, existing)| existing.shard == database.shard)
        {
            // A primary replaces whatever was picked for the shard first.
            Some(target) if target.1.role != Role::Primary => {
                if database.role == Role::Primary {
                    *target = (number, database);
                }
            }
            Some(_) => (),
            None => targets.push((number, database)),
        }
    }

    targets
}

/// Verify a client-supplied passthrough credential against the actual
/// PostgreSQL server before it is stored.
///
/// `entry` is the user entry that would be stored (see [`passthrough_entry`]),
/// so the check uses the same server user, password and auth mode the pool
/// would. When the pool would not use the client password as a server
/// credential (a `server_password` or database-level password is configured,
/// or an external identity provider is used), there is nothing to verify and
/// the credential is accepted as before.
///
/// Every shard of the database is checked, concurrently, because a credential
/// stored for the user poisons all of them. The results are combined
/// asymmetrically on purpose: a rejection from any shard rejects the login,
/// while a shard PgDog cannot reach at all does not, as long as another shard
/// accepted the credential. Requiring every shard to answer would mean one
/// unavailable shard blocks every new login, and a credential that turns out to
/// be wrong on a shard that was down is still evicted later by
/// [`passthrough_password_rejected`].
async fn verify_passthrough_credential(
    entry: &ConfigUser,
    config: &ConfigAndUsers,
) -> Verification {
    let Some(password) = entry.password.as_deref() else {
        return Verification::Failed;
    };

    let targets = verification_targets(entry, config);
    if targets.is_empty() {
        return Verification::NoDatabase;
    }

    let connect_timeout = Duration::from_millis(config.config.general.connect_timeout);
    let checks = targets.into_iter().map(|(number, database)| {
        let address = Address::new(database, entry, number);
        async move {
            // The pool would not use the client's password here, so there is
            // nothing this login could poison.
            if !address.passwords.iter().any(|p| p.as_str() == password) {
                return Verification::Ok;
            }

            match safe_timeout(
                connect_timeout,
                Box::pin(Server::connect(
                    &address,
                    ServerOptions::default(),
                    ConnectReason::PassthroughVerify,
                    Default::default(),
                )),
            )
            .await
            {
                Ok(Ok(_)) => Verification::Ok,
                Ok(Err(err)) if err.is_auth() => Verification::BadPassword,
                Ok(Err(_)) | Err(_) => Verification::Failed,
            }
        }
    });

    let results = join_all(checks).await;
    if results
        .iter()
        .any(|result| matches!(result, Verification::BadPassword))
    {
        Verification::BadPassword
    } else if results
        .iter()
        .any(|result| matches!(result, Verification::Ok))
    {
        Verification::Ok
    } else {
        Verification::Failed
    }
}

/// Authenticate a client through passthrough authentication.
///
/// Unlike [`add`], the credential is verified against the server before it is
/// stored: a mistyped client password gets a clean authentication error
/// instead of becoming the pool's server credential and poisoning every
/// connection attempt until it is replaced.
///
/// Each verification costs a server connection and the client decides when one
/// happens, so [`crate::backend::passthrough`] caps how many run at once and
/// answers a credential the server just rejected without asking again.
pub(crate) async fn add_passthrough(user: ConfigUser) -> Result<AuthResult, Error> {
    let config = config();
    match passthrough_action(&user, &config) {
        PassthroughAction::Match => Ok(AuthResult::Ok),
        PassthroughAction::Deny(result) => Ok(result),
        PassthroughAction::Store => {
            // Verification opens a server connection, and the client chooses
            // when that happens, so the same credential is not re-verified
            // while a rejection is fresh and only so many checks run at once.
            let attempt = Attempt::new(
                &user.name,
                &user.database,
                user.password.as_deref().unwrap_or_default(),
            );
            if throttle().rejected(&attempt) {
                debug!(
                    r#"passthrough credential for user "{}" on database "{}" was just rejected by the server"#,
                    user.name, user.database
                );
                return Ok(AuthResult::NoPasswordMatch);
            }

            let entry = passthrough_entry(user.clone(), &config);
            let _slot = throttle().slot().await;
            match verify_passthrough_credential(&entry, &config).await {
                Verification::Ok => add(user),
                Verification::BadPassword => {
                    throttle().reject(attempt);
                    Ok(AuthResult::NoPasswordMatch)
                }
                Verification::NoDatabase => Ok(AuthResult::NoUserOrDatabase),
                Verification::Failed => Ok(AuthResult::PassthroughVerificationFailed),
            }
        }
    }
}

/// Add new user to pool via passthrough authentication.
///
/// Return true if user can login, false otherwise.
///
pub(crate) fn add(user: ConfigUser) -> Result<AuthResult, Error> {
    fn add_user(user: ConfigUser) -> Result<(), Error> {
        debug!(
            r#"adding user "{}" to database "{}" via passthrough auth"#,
            user.name, user.database
        );

        let _lock = lock();
        let mut config = (*config()).clone();
        config.users.add_or_replace(user);
        set(config)?;

        Ok(())
    }

    let config = config();
    match passthrough_action(&user, &config) {
        PassthroughAction::Match => Ok(AuthResult::Ok),
        PassthroughAction::Deny(result) => Ok(result),
        PassthroughAction::Store => {
            add_user(passthrough_entry(user, &config))?;
            reload_from_existing()?;
            Ok(AuthResult::Ok)
        }
    }
}

/// Evict a passthrough-learned password the server has rejected.
///
/// Called when a server connection fails authentication with a credential
/// learned through passthrough authentication (e.g. the server-side password
/// rotated after the credential was stored). Eviction stops the pool from
/// retrying a password that can never work and lets the next client login
/// store the current one. Passwords configured in `users.toml` are never
/// touched.
pub(crate) fn passthrough_password_rejected(user_name: &str, database: &str, rejected: &str) {
    // Called from `Server::connect`, which runs on a client's critical path or
    // in the pool maintenance loop, while evicting rebuilds every pool
    // (`set` reruns configuration checks and reloads sharding centroids from
    // disk). Nothing waits on the result, and the eviction is idempotent -- it
    // re-reads the configuration and bails unless that exact credential is
    // still stored -- so hand it to a task of its own.
    let (user_name, database, rejected) = (
        user_name.to_owned(),
        database.to_owned(),
        rejected.to_owned(),
    );

    spawn(async move {
        evict_passthrough_password(&user_name, &database, &rejected);
    });
}

/// Evict a rejected passthrough credential. See
/// [`passthrough_password_rejected`], which is how callers reach this.
fn evict_passthrough_password(user_name: &str, database: &str, rejected: &str) {
    let evicted = {
        let _lock = lock();
        let config_now = config();
        let Some(user) = config_now
            .users
            .users
            .iter()
            .find(|user| user.name == user_name && user.database == database)
        else {
            return;
        };

        if !user.password_from_passthrough || user.password.as_deref() != Some(rejected) {
            return;
        }

        let mut config = (*config_now).clone();
        if user.created_by_passthrough {
            // The whole entry was discovered through passthrough
            // authentication: remove it, restoring the pre-discovery state.
            config
                .users
                .users
                .retain(|user| !(user.name == user_name && user.database == database));
        } else {
            let mut updated = user.clone();
            updated.password = None;
            updated.password_from_passthrough = false;
            config.users.add_or_replace(updated);
        }

        match set(config) {
            Ok(_) => true,
            Err(err) => {
                error!("error evicting rejected passthrough password: {}", err);
                false
            }
        }
    };

    if evicted {
        warn!(
            r#"evicted passthrough password for user "{}" on database "{}": the server rejected it"#,
            user_name, database
        );
        if let Err(err) = reload_from_existing() {
            error!(
                "error reloading after passthrough password eviction: {}",
                err
            );
        }
    }
}

/// Swap database configs between source and destination.
/// Both databases keep their names, but their configs (host, port, etc.) are exchanged.
/// User database references are also swapped.
/// Persists changes to disk (best effort).
pub(crate) async fn cutover(source: &str, destination: &str) -> Result<(), Error> {
    use tokio::fs::{copy, write};

    let config = {
        let _lock = lock();

        let mut config = config().deref().clone();

        config.config.cutover(source, destination);
        config.users.cutover(source, destination);

        let databases = from_config(&config);

        replace_databases(databases, true)?;

        config
    };

    info!(r#"databases swapped: "{}" <-> "{}""#, source, destination);

    if config.config.general.cutover_save_config {
        if let Err(err) = copy(
            &config.config_path,
            config.config_path.clone().with_extension("bak.toml"),
        )
        .await
        {
            warn!(
                "{} is read-only, skipping config persistence (err: {})",
                config
                    .config_path
                    .parent()
                    .map(|path| path.to_owned())
                    .unwrap_or_default()
                    .display(),
                err
            );
            return Ok(());
        }

        copy(
            &config.users_path,
            &config.users_path.clone().with_extension("bak.toml"),
        )
        .await?;

        write(
            &config.config_path,
            toml::to_string_pretty(&config.config)?.as_bytes(),
        )
        .await?;

        write(
            &config.users_path,
            toml::to_string_pretty(&config.users)?.as_bytes(),
        )
        .await?;
    }

    Ok(())
}

pub(crate) use pgdog_stats::User;

/// Convert to a database/user pair.
pub(crate) trait ToUser {
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
pub(crate) struct Databases {
    databases: HashMap<User, Cluster>,
    mirrors: HashMap<User, Vec<Cluster>>,
    mirror_configs: HashMap<(String, String), crate::config::MirrorConfig>,
}

impl Databases {
    /// Get the database user password, if one is configured.
    pub(crate) fn passwords(&self, user: impl ToUser) -> Option<&[PasswordKind]> {
        if let Some(cluster) = self.databases.get(&user.to_user()) {
            if cluster.passwords().is_empty() {
                None
            } else {
                Some(cluster.passwords())
            }
        } else {
            None
        }
    }

    /// Get a cluster for the user/database pair if it's configured.
    pub(crate) fn cluster(&self, user: impl ToUser) -> Result<Cluster, Error> {
        let user = user.to_user();
        if let Some(cluster) = self.databases.get(&user) {
            Ok(cluster.clone())
        } else {
            Err(Error::NoDatabase(user.clone()))
        }
    }

    /// Get the schema owner for this database.
    pub(crate) fn schema_owner(&self, database: &str) -> Result<Cluster, Error> {
        for (user, cluster) in &self.databases {
            if cluster.schema_admin() && user.database == database {
                return Ok(cluster.clone());
            }
        }

        Err(Error::NoSchemaOwner(database.to_owned()))
    }

    /// Get all schema owners for all databases,
    /// one per database.
    ///
    /// N.B.: Subsequent entry will override previous entry.
    ///
    pub(crate) fn schema_owners(&self) -> Vec<Cluster> {
        let mut schema_owners = HashMap::new();

        for cluster in self.databases.values() {
            if cluster.schema_admin() {
                schema_owners.insert(cluster.name().to_string(), cluster.clone());
            }
        }

        schema_owners.into_values().collect()
    }

    pub(crate) fn mirrors(&self, user: impl ToUser) -> Result<Option<&[Cluster]>, Error> {
        let user = user.to_user();
        if self.databases.contains_key(&user) {
            Ok(self.mirrors.get(&user).map(|m| m.as_slice()))
        } else {
            Err(Error::NoDatabase(user.clone()))
        }
    }

    /// Get precomputed mirror configuration.
    pub(crate) fn mirror_config(
        &self,
        source_db: &str,
        destination_db: &str,
    ) -> Option<&crate::config::MirrorConfig> {
        self.mirror_configs
            .get(&(source_db.to_string(), destination_db.to_string()))
    }

    /// Get all clusters and databases.
    pub(crate) fn all(&self) -> &HashMap<User, Cluster> {
        &self.databases
    }

    /// Cancel a query running on one of the databases proxied by the pooler.
    pub(crate) async fn cancel(&self, id: FrontendPid) -> Result<(), Error> {
        for cluster in self.databases.values() {
            cluster.cancel(id).await?;
        }

        Ok(())
    }

    /// Move all connections we can from old databases config to new
    /// databases config.
    pub(crate) fn move_conns_to(&self, destination: &Databases) -> Result<usize, Error> {
        let mut moved = 0;
        for (user, cluster) in &self.databases {
            let dest = destination.databases.get(user);

            if let Some(dest) = dest
                && cluster.can_move_conns_to(dest)
                && cluster.move_conns_to(dest)?
            {
                moved += 1;
            }
        }

        Ok(moved)
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
            if cluster.passwords().is_empty() && cluster.identity().is_none() {
                warn!(
                    r#"disabling pool for user "{}" and database "{}", password not set"#,
                    cluster.user(),
                    cluster.name()
                );
                // No boot-time maintenance will run, don't block
                // readiness waiters. Checkouts will fail instead.
                cluster.mark_ready();
            } else {
                cluster.launch();
            }

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

fn resolve_sharded_table(
    config: &ShardedTableConfig,
    mappings: &IndexMap<ShardedMappingKey, Vec<ShardedMappingDeprecated>>,
    num_shards: usize,
) -> ShardedTable {
    let mapping = config
        .mapping
        .clone()
        .or_else(|| resolve_table_mapping_deprecated(config, mappings));

    let mapping = mapping.map(|configs| {
        let tname = config.name.as_deref().unwrap_or("*");
        let column = &config.column;
        for error in crate::backend::validation::validate(&configs, config.data_type, num_shards) {
            warn!("sharded table name=\"{tname}\", column=\"{column}\": {error}");
        }
        Mapping::new(configs)
    });

    ShardedTable {
        database: config.database.clone(),
        name: config.name.as_deref().map(normalize_identifier),
        schema: config.schema.as_deref().map(normalize_identifier),
        column: normalize_identifier(&config.column),
        primary: config.primary,
        centroids: config.centroids.clone(),
        data_type: config.data_type,
        centroid_probes: config.centroid_probes,
        hasher: config.hasher.clone(),
        mapping: mapping.flatten(),
        lookup_query: config.lookup_query.clone(),
        lookup_result: config.lookup_result,
    }
}

fn resolve_table_mapping_deprecated(
    table: &ShardedTableConfig,
    mappings: &IndexMap<ShardedMappingKey, Vec<ShardedMappingDeprecated>>,
) -> Option<Vec<ShardedMappingConfig>> {
    let found = mappings.get(&ShardedMappingKeyRef {
        database: &table.database,
        column: &table.column,
        table: table.name.as_ref(),
    })?;

    Some(
        found
            .iter()
            .map(|map| match map.kind {
                ShardedMappingKindDeprecated::List => {
                    ShardedMappingConfig::List(ShardedMappingList {
                        shard: map.shard,
                        values: map.values.clone(),
                    })
                }
                ShardedMappingKindDeprecated::Range => {
                    ShardedMappingConfig::Range(ShardedMappingRange {
                        shard: map.shard,
                        start: map.start.clone(),
                        end: map.end.clone(),
                    })
                }
                ShardedMappingKindDeprecated::Default => {
                    ShardedMappingConfig::Default { shard: map.shard }
                }
            })
            .collect(),
    )
}

// Create new Cluster from user and databases in `pgdog.toml`.
//
// # Arguments
//
// - `user`: `[[users]]` entry in `users.toml`
// - `config`: all of `pgdog.toml`
// - `schema_cache`: A cache of database tables, shared between all clusters. This is passed here
//                   to ensure all clusters share the same schema cache, and to make sure a new one
//                   is created on each config reload.
fn new_pool(
    user: &crate::config::User,
    config: &crate::config::Config,
    schema_cache: SchemaCache,
) -> Option<(User, Cluster)> {
    let omnisharded_tables = config.omnisharded_tables();
    let sharded_mappings = config.sharded_mappings();
    let sharded_schemas = config.sharded_schemas();
    let general = &config.general;
    let databases = config.databases();

    let shards = databases.get(&user.database)?;

    let shard_configs: Vec<ClusterShardConfig> = shards
        .iter()
        .map(|entries| {
            let shard = ShardNodes::new(entries);
            let pool = |database: &EnumeratedDatabase| PoolConfig {
                address: Address::new(database, user, database.number),
                config: pgdog_config::pool::PoolConfig::resolve(
                    general,
                    &shard,
                    &database.database,
                    user,
                ),
            };

            ClusterShardConfig {
                primary: shard.primary().map(&pool),
                replicas: shard.replicas().map(&pool).collect(),
            }
        })
        .collect();

    let sharded_tables: Vec<_> = config
        .sharded_tables
        .iter()
        .filter(|t| t.database == user.database)
        .map(|t| resolve_sharded_table(t, &sharded_mappings, shard_configs.len()))
        .collect();
    let sharded_schemas = sharded_schemas
        .get(&user.database)
        .cloned()
        .unwrap_or_default();

    let omnisharded_tables = omnisharded_tables
        .get(&user.database)
        .cloned()
        .unwrap_or(vec![]);
    let sharded_tables = ShardedTables::new(
        sharded_tables,
        omnisharded_tables,
        general.omnisharded_sticky,
        general.system_catalogs,
    );
    let sharded_schemas = ShardedSchemas::new(sharded_schemas);
    let query_parser = config
        .query_parsers
        .iter()
        .find(|config| config.database == user.database)
        .cloned()
        .unwrap_or(QueryParser {
            database: user.database.clone(),
            level: config.general.query_parser,
            engine: config.general.query_parser_engine,
        });

    let cluster_config = ClusterConfig::new(
        config,
        user,
        &shard_configs,
        sharded_tables,
        sharded_schemas,
        query_parser,
        schema_cache,
    );

    Some((
        User {
            user: user.name.clone(),
            database: user.database.clone(),
        },
        Cluster::new(cluster_config),
    ))
}

/// Load databases from config.
pub(crate) fn from_config(config: &ConfigAndUsers) -> Databases {
    let mut databases = HashMap::new();
    // The schema cache is shared between all databases.
    let schema_cache = SchemaCache::default();

    for user in &config.users.users {
        for database in config.config.user_databases(user) {
            let mut user = user.clone();
            // FIXME: this is a hacky way to specify a single database entry
            // through the user, since user can have different configs
            user.databases.clear();
            user.database = database;

            if let Some((user, cluster)) = new_pool(&user, &config.config, schema_cache.clone()) {
                databases.insert(user, cluster);
            }
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
                level: mirror.level,
            };
            mirror_configs.insert(
                (mirror.source_db.clone(), mirror.destination_db.clone()),
                mirror_config,
            );
        }
    }

    Databases {
        databases,
        mirrors,
        mirror_configs,
    }
}

#[cfg(test)]
mod tests {
    use pgdog_config::{General, Mirroring, PassthroughAuth};

    use tokio::time::sleep;

    use super::*;
    use crate::config::{Config, ConfigAndUsers, Database, Role};

    fn setup_config(passthrough_auth: PassthroughAuth, users: Vec<ConfigUser>) {
        let _lock = lock();
        let config = Config {
            databases: vec![Database {
                name: "db1".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                role: Role::Primary,
                ..Default::default()
            }],
            general: General {
                passthrough_auth,
                ..Default::default()
            },
            ..Default::default()
        };

        let users = crate::config::Users {
            users,
            ..Default::default()
        };

        let cu = ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
        };

        crate::config::set(cu).expect("set config");
        let databases = from_config(&crate::config::config());
        replace_databases(databases, false).expect("replace databases");
    }

    fn make_user(name: &str, password: Option<&str>) -> ConfigUser {
        ConfigUser {
            name: name.to_string(),
            database: "db1".to_string(),
            password: password.map(|p| p.to_string()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_add_new_user() {
        setup_config(PassthroughAuth::EnabledPlain, vec![]);

        let result = add(make_user("new_user", Some("secret")));
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());

        let config = crate::config::config();
        let found = config.users.find(&make_user("new_user", None));
        assert!(found.is_some());
        assert_eq!(found.unwrap().password, Some("secret".to_string()));
    }

    #[tokio::test]
    async fn test_add_existing_user_matching_password() {
        setup_config(
            PassthroughAuth::EnabledPlain,
            vec![make_user("alice", Some("pass123"))],
        );

        let result = add(make_user("alice", Some("pass123")));
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_add_existing_user_no_password_set() {
        setup_config(PassthroughAuth::EnabledPlain, vec![make_user("bob", None)]);

        let result = add(make_user("bob", Some("new_pass")));
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());

        let config = crate::config::config();
        let found = config.users.find(&make_user("bob", None));
        assert_eq!(found.unwrap().password, Some("new_pass".to_string()));
    }

    #[tokio::test]
    async fn test_add_existing_user_wrong_password_no_change_allowed() {
        setup_config(
            PassthroughAuth::EnabledPlain,
            vec![make_user("charlie", Some("old_pass"))],
        );

        let result = add(make_user("charlie", Some("wrong_pass")));
        assert!(result.is_ok());
        assert!(!result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_add_existing_user_wrong_password_change_allowed() {
        setup_config(
            PassthroughAuth::EnabledPlainAllowChange,
            vec![make_user("dave", Some("old_pass"))],
        );

        let result = add(make_user("dave", Some("new_pass")));
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());

        let config = crate::config::config();
        let found = config.users.find(&make_user("dave", None));
        assert_eq!(found.unwrap().password, Some("new_pass".to_string()));
    }

    #[tokio::test]
    async fn test_add_marks_passthrough_credentials() {
        setup_config(PassthroughAuth::EnabledPlain, vec![make_user("bob", None)]);

        // Discovered user: whole entry is removable on rejection.
        add(make_user("new_user", Some("secret"))).expect("add");
        let found = crate::config::config()
            .users
            .find(&make_user("new_user", None))
            .expect("user added");
        assert!(found.password_from_passthrough);
        assert!(found.created_by_passthrough);

        // Configured user with no password: only the password is learned.
        add(make_user("bob", Some("secret"))).expect("add");
        let found = crate::config::config()
            .users
            .find(&make_user("bob", None))
            .expect("user exists");
        assert!(found.password_from_passthrough);
        assert!(!found.created_by_passthrough);
    }

    #[tokio::test]
    async fn test_add_password_change_keeps_configured_settings() {
        let mut dave = make_user("dave", Some("old_pass"));
        dave.pool_size = Some(7);
        setup_config(PassthroughAuth::EnabledPlainAllowChange, vec![dave]);

        add(make_user("dave", Some("new_pass"))).expect("add");

        let found = crate::config::config()
            .users
            .find(&make_user("dave", None))
            .expect("user exists");
        assert_eq!(found.password, Some("new_pass".to_string()));
        assert_eq!(found.pool_size, Some(7));
        assert!(found.password_from_passthrough);
        assert!(!found.created_by_passthrough);
    }

    #[tokio::test]
    async fn test_passthrough_rejected_removes_discovered_user() {
        setup_config(PassthroughAuth::EnabledPlain, vec![]);

        add(make_user("eve", Some("stale"))).expect("add");
        evict_passthrough_password("eve", "db1", "stale");

        let config = crate::config::config();
        assert!(config.users.find(&make_user("eve", None)).is_none());
    }

    #[tokio::test]
    async fn test_passthrough_rejected_clears_learned_password() {
        setup_config(PassthroughAuth::EnabledPlain, vec![make_user("bob", None)]);

        add(make_user("bob", Some("stale"))).expect("add");
        evict_passthrough_password("bob", "db1", "stale");

        let found = crate::config::config()
            .users
            .find(&make_user("bob", None))
            .expect("configured entry kept");
        assert_eq!(found.password, None);
        assert!(!found.password_from_passthrough);
    }

    #[tokio::test]
    async fn test_passthrough_rejected_ignores_configured_password() {
        setup_config(
            PassthroughAuth::EnabledPlain,
            vec![make_user("alice", Some("configured"))],
        );

        evict_passthrough_password("alice", "db1", "configured");

        let found = crate::config::config()
            .users
            .find(&make_user("alice", None))
            .expect("user exists");
        assert_eq!(found.password, Some("configured".to_string()));
    }

    #[tokio::test]
    async fn test_passthrough_rejected_ignores_stale_password() {
        setup_config(PassthroughAuth::EnabledPlain, vec![]);

        add(make_user("eve", Some("current"))).expect("add");
        evict_passthrough_password("eve", "db1", "previous");

        let found = crate::config::config()
            .users
            .find(&make_user("eve", None))
            .expect("user kept");
        assert_eq!(found.password, Some("current".to_string()));
    }

    /// `Server::connect` only hands the eviction off, so it happens in a task
    /// of its own rather than on the connection's critical path.
    #[tokio::test]
    async fn test_passthrough_rejected_evicts_in_the_background() {
        setup_config(PassthroughAuth::EnabledPlain, vec![]);

        add(make_user("eve", Some("stale"))).expect("add");
        passthrough_password_rejected("eve", "db1", "stale");

        for _ in 0..100 {
            if crate::config::config()
                .users
                .find(&make_user("eve", None))
                .is_none()
            {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }

        panic!("rejected passthrough password was never evicted");
    }

    /// Config pointing at a closed port: any verification attempt fails, so
    /// these tests can tell whether one was made.
    fn setup_unreachable_config(users: Vec<ConfigUser>) {
        let _lock = lock();
        let config = Config {
            databases: vec![Database {
                name: "db1".to_string(),
                host: "127.0.0.1".to_string(),
                port: 1,
                role: Role::Primary,
                ..Default::default()
            }],
            general: General {
                passthrough_auth: PassthroughAuth::EnabledPlain,
                ..Default::default()
            },
            ..Default::default()
        };
        let cu = ConfigAndUsers {
            config,
            users: crate::config::Users {
                users,
                ..Default::default()
            },
            ..Default::default()
        };
        crate::config::set(cu).expect("set config");
        replace_databases(from_config(&crate::config::config()), false).expect("replace");
    }

    #[tokio::test]
    async fn test_add_passthrough_unreachable_server_stores_nothing() {
        // Port 1 is closed: verification can't run, so the credential must
        // not be stored and the client gets a verification error.
        setup_unreachable_config(vec![]);

        let result = add_passthrough(make_user("new_user", Some("secret")))
            .await
            .expect("add_passthrough");
        assert_eq!(result, AuthResult::PassthroughVerificationFailed);
        assert!(
            crate::config::config()
                .users
                .find(&make_user("new_user", None))
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_add_passthrough_skips_verification_with_server_password() {
        // The configured entry logs into the server with `server_password`,
        // so the client password is never a server credential: it is stored
        // without contacting the (unreachable) server.
        let mut alice = make_user("alice", None);
        alice.server_password = Some("service".to_string());
        setup_unreachable_config(vec![alice]);

        let result = add_passthrough(make_user("alice", Some("client_pw")))
            .await
            .expect("add_passthrough");
        assert_eq!(result, AuthResult::Ok);
        let found = crate::config::config()
            .users
            .find(&make_user("alice", None))
            .expect("user exists");
        assert_eq!(found.password, Some("client_pw".to_string()));
        assert_eq!(found.server_password, Some("service".to_string()));
    }

    #[tokio::test]
    async fn test_add_passthrough_no_database_stores_nothing() {
        setup_config(PassthroughAuth::EnabledPlain, vec![]);

        let mut user = make_user("new_user", Some("secret"));
        user.database = "does_not_exist".to_string();
        let result = add_passthrough(user).await.expect("add_passthrough");
        assert_eq!(result, AuthResult::NoUserOrDatabase);
    }

    #[tokio::test]
    async fn test_add_passthrough_denies_password_change() {
        setup_config(
            PassthroughAuth::EnabledPlain,
            vec![make_user("alice", Some("configured"))],
        );

        // No server connection needed: rejected before verification.
        let result = add_passthrough(make_user("alice", Some("other")))
            .await
            .expect("add_passthrough");
        assert_eq!(result, AuthResult::NoPassthroughPasswordChange);
    }

    fn setup_live_config(users: Vec<ConfigUser>) {
        let _lock = lock();
        let config = Config {
            databases: vec![Database {
                name: "pgdog".to_string(),
                host: "127.0.0.1".to_string(),
                port: 5432,
                role: Role::Primary,
                ..Default::default()
            }],
            general: General {
                passthrough_auth: PassthroughAuth::EnabledPlain,
                ..Default::default()
            },
            ..Default::default()
        };
        let cu = ConfigAndUsers {
            config,
            users: crate::config::Users {
                users,
                ..Default::default()
            },
            ..Default::default()
        };
        crate::config::set(cu).expect("set config");
        replace_databases(from_config(&crate::config::config()), false).expect("replace");
    }

    #[tokio::test]
    async fn test_add_passthrough_verifies_against_server() {
        setup_live_config(vec![]);

        // Wrong password: rejected by the server, nothing stored.
        let mut user = make_user("pgdog", Some("wrong_password"));
        user.database = "pgdog".to_string();
        let result = add_passthrough(user).await.expect("add_passthrough");
        assert_eq!(result, AuthResult::NoPasswordMatch);
        let mut lookup = make_user("pgdog", None);
        lookup.database = "pgdog".to_string();
        assert!(crate::config::config().users.find(&lookup).is_none());

        // Correct password: verified and stored.
        let mut user = make_user("pgdog", Some("pgdog"));
        user.database = "pgdog".to_string();
        let result = add_passthrough(user).await.expect("add_passthrough");
        assert_eq!(result, AuthResult::Ok);
        let found = crate::config::config()
            .users
            .find(&lookup)
            .expect("user stored");
        assert!(found.password_from_passthrough);
        assert_eq!(found.password, Some("pgdog".to_string()));
    }

    /// A credential the server just rejected is refused without asking the
    /// server again, so a client reconnecting in a loop with a wrong password
    /// costs one connection, not one per attempt.
    #[tokio::test]
    async fn test_add_passthrough_does_not_reverify_a_rejected_credential() {
        setup_live_config(vec![]);

        // A credential no other test uses, since the throttle is process-wide.
        let credential = "rejected_then_throttled";
        let mut user = make_user("pgdog", Some(credential));
        user.database = "pgdog".to_string();

        let result = add_passthrough(user.clone()).await.expect("first attempt");
        assert_eq!(result, AuthResult::NoPasswordMatch);

        // Point the configuration at a database that does not exist. Reaching
        // verification now would answer NoUserOrDatabase, so answering
        // NoPasswordMatch again shows the attempt never got that far.
        setup_config(PassthroughAuth::EnabledPlain, vec![]);
        let result = add_passthrough(user).await.expect("second attempt");
        assert_eq!(result, AuthResult::NoPasswordMatch);
    }

    /// Two shards, one of them unreachable. A shard that cannot be reached
    /// must not block a login, but a password the reachable shard rejects
    /// must still be refused.
    fn setup_sharded_config() {
        let _lock = lock();
        let shard = |shard: usize, port: u16| Database {
            name: "pgdog".to_string(),
            host: "127.0.0.1".to_string(),
            port,
            shard,
            role: Role::Primary,
            ..Default::default()
        };
        let config = Config {
            databases: vec![shard(0, 5432), shard(1, 1)],
            general: General {
                passthrough_auth: PassthroughAuth::EnabledPlain,
                ..Default::default()
            },
            ..Default::default()
        };
        let cu = ConfigAndUsers {
            config,
            users: crate::config::Users::default(),
            ..Default::default()
        };
        crate::config::set(cu).expect("set config");
    }

    #[tokio::test]
    async fn test_add_passthrough_verifies_every_shard() {
        setup_sharded_config();

        let mut user = make_user("pgdog", Some("pgdog"));
        user.database = "pgdog".to_string();
        let entry = passthrough_entry(user.clone(), &crate::config::config());

        // One target per shard, each the shard's primary.
        let config = crate::config::config();
        let targets = verification_targets(&entry, &config);
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].1.shard, 0);
        assert_eq!(targets[1].1.shard, 1);

        // Shard 1 is unreachable, shard 0 accepts: the login goes through
        // rather than being held up by the shard that is down.
        assert!(matches!(
            verify_passthrough_credential(&entry, &config).await,
            Verification::Ok
        ));

        // The reachable shard rejecting the password is authoritative.
        let mut wrong = make_user("pgdog", Some("wrong_password"));
        wrong.database = "pgdog".to_string();
        let wrong = passthrough_entry(wrong, &config);
        assert!(matches!(
            verify_passthrough_credential(&wrong, &config).await,
            Verification::BadPassword
        ));
    }

    /// A shard with a replica entry as well: the primary is the one verified
    /// against, whatever order the entries appear in.
    #[tokio::test]
    async fn test_verification_targets_prefer_the_primary() {
        let entry = ConfigUser {
            name: "pgdog".to_string(),
            database: "pgdog".to_string(),
            ..Default::default()
        };
        let database = |shard: usize, port: u16, role: Role| Database {
            name: "pgdog".to_string(),
            host: "127.0.0.1".to_string(),
            port,
            shard,
            role,
            ..Default::default()
        };

        let config = ConfigAndUsers {
            config: Config {
                databases: vec![
                    database(0, 5001, Role::Replica),
                    database(0, 5000, Role::Primary),
                    database(1, 5002, Role::Replica),
                    Database {
                        name: "other".to_string(),
                        ..database(0, 5003, Role::Primary)
                    },
                ],
                ..Default::default()
            },
            ..Default::default()
        };

        let targets = verification_targets(&entry, &config);
        assert_eq!(targets.len(), 2);
        // Shard 0 resolves to the primary, shard 1 has only a replica.
        assert_eq!(targets[0].1.port, 5000);
        assert_eq!(targets[1].1.port, 5002);
    }

    #[test]
    fn test_mirror_user_isolation() {
        // Test that each user gets their own mirror cluster
        let config = Config {
            databases: vec![
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
            ],
            mirroring: vec![Mirroring {
                source_db: "db1".to_string(),
                destination_db: "db1_mirror".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

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
            ..Default::default()
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
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
        let config = Config {
            databases: vec![
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
            ],
            mirroring: vec![Mirroring {
                source_db: "source_db".to_string(),
                destination_db: "dest_db".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

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
            ..Default::default()
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
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

        config.mirroring = vec![Mirroring {
            source_db: "source_db".to_string(),
            destination_db: "dest_db".to_string(),
            queue_length: Some(256),
            exposure: Some(0.5),
            ..Default::default()
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
            ..Default::default()
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
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
        config.mirroring = vec![Mirroring {
            source_db: "db1".to_string(),
            destination_db: "db2".to_string(),
            ..Default::default()
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
            ..Default::default()
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
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
            Mirroring {
                source_db: "primary".to_string(),
                destination_db: "mirror1".to_string(),
                queue_length: Some(200), // Override queue only
                ..Default::default()
            },
            Mirroring {
                source_db: "primary".to_string(),
                destination_db: "mirror2".to_string(),
                exposure: Some(0.25), // Override exposure only
                ..Default::default()
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
            ..Default::default()
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
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
        let config = Config {
            databases: vec![
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
            ],
            mirroring: vec![Mirroring {
                source_db: "source".to_string(),
                destination_db: "dest".to_string(),
                queue_length: Some(256),
                exposure: Some(0.5),
                ..Default::default()
            }],
            ..Default::default()
        };

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
            ..Default::default()
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
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
        config.mirroring = vec![Mirroring {
            source_db: "source_db".to_string(),
            destination_db: "dest_db".to_string(),
            queue_length: Some(256),
            exposure: Some(0.5),
            ..Default::default()
        }];

        // No users at all
        let users = crate::config::Users {
            users: vec![],
            ..Default::default()
        };

        let databases = from_config(&ConfigAndUsers {
            config: config.clone(),
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
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
            ..Default::default()
        };

        let databases_partial = from_config(&ConfigAndUsers {
            config: config.clone(),
            users: users_partial,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
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
            ..Default::default()
        };

        let databases_dest_only = from_config(&ConfigAndUsers {
            config,
            users: users_dest_only,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
        });

        // Mirror config should not be precomputed when source has no users
        let mirror_config_dest_only = databases_dest_only.mirror_config("source_db", "dest_db");
        assert!(
            mirror_config_dest_only.is_none(),
            "Mirror config should not be precomputed when source has no users"
        );
    }

    #[test]
    fn test_user_all_databases_creates_pools_for_all_dbs() {
        let config = Config {
            databases: vec![
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
                Database {
                    name: "db3".to_string(),
                    host: "localhost".to_string(),
                    port: 5434,
                    role: Role::Primary,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let users = crate::config::Users {
            users: vec![crate::config::User {
                name: "admin_user".to_string(),
                all_databases: true,
                password: Some("pass".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
        });

        // User should have pools for all three databases
        assert!(
            databases.cluster(("admin_user", "db1")).is_ok(),
            "admin_user should have access to db1"
        );
        assert!(
            databases.cluster(("admin_user", "db2")).is_ok(),
            "admin_user should have access to db2"
        );
        assert!(
            databases.cluster(("admin_user", "db3")).is_ok(),
            "admin_user should have access to db3"
        );

        // Verify exactly 3 pools were created
        assert_eq!(databases.all().len(), 3);
    }

    #[test]
    fn test_user_multiple_databases_creates_pools_for_specified_dbs() {
        let config = Config {
            databases: vec![
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
                Database {
                    name: "db3".to_string(),
                    host: "localhost".to_string(),
                    port: 5434,
                    role: Role::Primary,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let users = crate::config::Users {
            users: vec![crate::config::User {
                name: "limited_user".to_string(),
                databases: vec!["db1".to_string(), "db3".to_string()],
                password: Some("pass".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
        });

        // User should have pools for db1 and db3 only
        assert!(
            databases.cluster(("limited_user", "db1")).is_ok(),
            "limited_user should have access to db1"
        );
        assert!(
            databases.cluster(("limited_user", "db3")).is_ok(),
            "limited_user should have access to db3"
        );
        assert!(
            databases.cluster(("limited_user", "db2")).is_err(),
            "limited_user should NOT have access to db2"
        );

        // Verify exactly 2 pools were created
        assert_eq!(databases.all().len(), 2);
    }

    #[test]
    fn test_all_databases_takes_priority_over_databases_list() {
        let config = Config {
            databases: vec![
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
                Database {
                    name: "db3".to_string(),
                    host: "localhost".to_string(),
                    port: 5434,
                    role: Role::Primary,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        // User has both all_databases=true AND specific databases set
        let users = crate::config::Users {
            users: vec![crate::config::User {
                name: "mixed_user".to_string(),
                all_databases: true,
                databases: vec!["db1".to_string()], // Should be ignored
                password: Some("pass".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
        });

        // all_databases should take priority - user gets all 3 databases
        assert!(
            databases.cluster(("mixed_user", "db1")).is_ok(),
            "mixed_user should have access to db1"
        );
        assert!(
            databases.cluster(("mixed_user", "db2")).is_ok(),
            "mixed_user should have access to db2"
        );
        assert!(
            databases.cluster(("mixed_user", "db3")).is_ok(),
            "mixed_user should have access to db3"
        );

        assert_eq!(databases.all().len(), 3);
    }

    #[test]
    fn test_new_pool_returns_none_for_nonexistent_database() {
        let config = Config::default(); // No databases configured

        let user = crate::config::User {
            name: "test_user".to_string(),
            database: "nonexistent_db".to_string(),
            password: Some("pass".to_string()),
            ..Default::default()
        };

        let result = new_pool(&user, &config, SchemaCache::default());
        assert!(
            result.is_none(),
            "new_pool should return None when database doesn't exist"
        );
    }

    #[test]
    fn test_user_with_single_database_creates_one_pool() {
        let config = Config {
            databases: vec![
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
            ],
            ..Default::default()
        };

        let users = crate::config::Users {
            users: vec![crate::config::User {
                name: "single_db_user".to_string(),
                database: "db1".to_string(),
                password: Some("pass".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
        });

        assert!(
            databases.cluster(("single_db_user", "db1")).is_ok(),
            "single_db_user should have access to db1"
        );
        assert!(
            databases.cluster(("single_db_user", "db2")).is_err(),
            "single_db_user should NOT have access to db2"
        );

        assert_eq!(databases.all().len(), 1);
    }

    #[test]
    fn test_multiple_users_with_different_database_access() {
        let config = Config {
            databases: vec![
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
                Database {
                    name: "db3".to_string(),
                    host: "localhost".to_string(),
                    port: 5434,
                    role: Role::Primary,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let users = crate::config::Users {
            users: vec![
                crate::config::User {
                    name: "admin".to_string(),
                    all_databases: true,
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                crate::config::User {
                    name: "limited".to_string(),
                    databases: vec!["db1".to_string(), "db2".to_string()],
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
                crate::config::User {
                    name: "single".to_string(),
                    database: "db3".to_string(),
                    password: Some("pass".to_string()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
        });

        // Admin has all 3 databases
        assert!(databases.cluster(("admin", "db1")).is_ok());
        assert!(databases.cluster(("admin", "db2")).is_ok());
        assert!(databases.cluster(("admin", "db3")).is_ok());

        // Limited has db1 and db2
        assert!(databases.cluster(("limited", "db1")).is_ok());
        assert!(databases.cluster(("limited", "db2")).is_ok());
        assert!(databases.cluster(("limited", "db3")).is_err());

        // Single has only db3
        assert!(databases.cluster(("single", "db1")).is_err());
        assert!(databases.cluster(("single", "db2")).is_err());
        assert!(databases.cluster(("single", "db3")).is_ok());

        // Total pools: admin(3) + limited(2) + single(1) = 6
        assert_eq!(databases.all().len(), 6);
    }

    #[test]
    fn test_databases_list_with_nonexistent_database_skipped() {
        let config = Config {
            databases: vec![Database {
                name: "db1".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                role: Role::Primary,
                ..Default::default()
            }],
            ..Default::default()
        };

        // User requests access to both existing and non-existing databases
        let users = crate::config::Users {
            users: vec![crate::config::User {
                name: "test_user".to_string(),
                databases: vec!["db1".to_string(), "nonexistent".to_string()],
                password: Some("pass".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };

        let databases = from_config(&ConfigAndUsers {
            config,
            users,
            config_path: std::path::PathBuf::new(),
            users_path: std::path::PathBuf::new(),
            ..Default::default()
        });

        // Should only create pool for db1, nonexistent is silently skipped
        assert!(databases.cluster(("test_user", "db1")).is_ok());
        assert!(databases.cluster(("test_user", "nonexistent")).is_err());

        assert_eq!(databases.all().len(), 1);
    }

    #[tokio::test]
    async fn test_cutover_persists_to_disk() {
        use tempfile::TempDir;
        use tokio::fs;

        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("pgdog.toml");
        let users_path = temp_dir.path().join("users.toml");

        let original_config = r#"
[[databases]]
name = "source_db"
host = "127.0.0.1"
port = 5432
role = "primary"

[[databases]]
name = "destination_db"
host = "127.0.0.2"
port = 5433
role = "primary"
"#;

        let original_users = r#"
[[users]]
name = "testuser"
database = "source_db"
password = "testpass"
"#;

        fs::write(&config_path, original_config).await.unwrap();
        fs::write(&users_path, original_users).await.unwrap();

        // Load config from temp files and set in global state
        let mut config = crate::config::ConfigAndUsers::load(&config_path, &users_path).unwrap();
        config.config.general.cutover_save_config = true;
        crate::config::set(config).unwrap();

        // Call the actual cutover function
        cutover("source_db", "destination_db").await.unwrap();

        // Verify backup files contain original content
        let backup_config = fs::read_to_string(config_path.with_extension("bak.toml"))
            .await
            .unwrap();
        let backup_config: crate::config::Config = toml::from_str(&backup_config).unwrap();
        let backup_source = backup_config
            .databases
            .iter()
            .find(|d| d.name == "source_db")
            .unwrap();
        assert_eq!(backup_source.host, "127.0.0.1");
        assert_eq!(backup_source.port, 5432);
        let backup_dest = backup_config
            .databases
            .iter()
            .find(|d| d.name == "destination_db")
            .unwrap();
        assert_eq!(backup_dest.host, "127.0.0.2");
        assert_eq!(backup_dest.port, 5433);

        let backup_users = fs::read_to_string(users_path.with_extension("bak.toml"))
            .await
            .unwrap();
        let backup_users: crate::config::Users = toml::from_str(&backup_users).unwrap();
        assert_eq!(backup_users.users.len(), 1);
        assert_eq!(backup_users.users[0].name, "testuser");
        assert_eq!(backup_users.users[0].database, "source_db");

        // Verify new config files have swapped values
        let new_config = fs::read_to_string(&config_path).await.unwrap();
        let new_config: crate::config::Config = toml::from_str(&new_config).unwrap();
        let new_source = new_config
            .databases
            .iter()
            .find(|d| d.name == "source_db")
            .unwrap();
        assert_eq!(new_source.host, "127.0.0.2");
        assert_eq!(new_source.port, 5433);
        let new_dest = new_config
            .databases
            .iter()
            .find(|d| d.name == "destination_db")
            .unwrap();
        assert_eq!(new_dest.host, "127.0.0.1");
        assert_eq!(new_dest.port, 5432);

        // Verify users were swapped
        let new_users = fs::read_to_string(&users_path).await.unwrap();
        let new_users: crate::config::Users = toml::from_str(&new_users).unwrap();
        assert_eq!(new_users.users.len(), 1);
        assert_eq!(new_users.users[0].name, "testuser");
        assert_eq!(new_users.users[0].database, "destination_db");
    }

    /// PostgreSQL folds unquoted identifiers to lower case, so the parser
    /// hands the router `orders` for `FROM Orders`. Identifiers configured
    /// in `pgdog.toml` must be folded the same way, otherwise they never
    /// match and the table silently isn't sharded.
    #[test]
    fn test_unquoted_config_identifiers_are_folded() {
        let config = ShardedTableConfig {
            database: "pgdog".into(),
            name: Some("Orders".into()),
            schema: Some("Public".into()),
            column: "Tenant_Id".into(),
            ..Default::default()
        };

        let resolved = resolve_sharded_table(&config, &IndexMap::new(), 2);

        assert_eq!(resolved.name.as_deref(), Some("orders"));
        assert_eq!(resolved.schema.as_deref(), Some("public"));
        assert_eq!(resolved.column, "tenant_id");
    }

    /// Quoted identifiers keep their case, and the surrounding quotes are
    /// not part of the identifier itself.
    #[test]
    fn test_quoted_config_identifiers_preserve_case() {
        let config = ShardedTableConfig {
            database: "pgdog".into(),
            name: Some(r#""Orders""#.into()),
            schema: Some(r#""Public""#.into()),
            column: r#""Tenant_Id""#.into(),
            ..Default::default()
        };

        let resolved = resolve_sharded_table(&config, &IndexMap::new(), 2);

        assert_eq!(resolved.name.as_deref(), Some("Orders"));
        assert_eq!(resolved.schema.as_deref(), Some("Public"));
        assert_eq!(resolved.column, "Tenant_Id");
    }
}
