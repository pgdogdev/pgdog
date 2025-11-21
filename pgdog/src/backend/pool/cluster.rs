//! A collection of replicas and a primary.

use parking_lot::{Mutex, RwLock};
use pgdog_config::{PreparedStatements, Rewrite};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::spawn;
use tracing::{error, info};

use crate::{
    backend::{
        databases::{databases, User as DatabaseUser},
        pool::shard::role_detector::DetectedRoles,
        replication::{ReplicationConfig, ShardedColumn, ShardedSchemas},
        Schema, ShardedTables,
    },
    config::{
        ConnectionRecovery, General, MultiTenant, PoolerMode, ReadWriteSplit, ReadWriteStrategy,
        ShardedTable, User,
    },
    net::{messages::BackendKeyData, Query},
};

use super::{Address, Config, Error, Guard, MirrorStats, Request, Shard, ShardConfig};
use crate::config::LoadBalancingStrategy;

#[derive(Clone, Debug, Default)]
/// Database configuration.
pub struct PoolConfig {
    /// Database address.
    pub(crate) address: Address,
    /// Pool settings.
    pub(crate) config: Config,
}

/// A collection of sharded replicas and primaries
/// belonging to the same database cluster.
#[derive(Clone, Default, Debug)]
pub struct Cluster {
    identifier: Arc<DatabaseUser>,
    shards: Vec<Shard>,
    password: String,
    pooler_mode: PoolerMode,
    sharded_tables: ShardedTables,
    sharded_schemas: ShardedSchemas,
    replication_sharding: Option<String>,
    schema: Arc<RwLock<Schema>>,
    multi_tenant: Option<MultiTenant>,
    rw_strategy: ReadWriteStrategy,
    schema_admin: bool,
    stats: Arc<Mutex<MirrorStats>>,
    cross_shard_disabled: bool,
    two_phase_commit: bool,
    two_phase_commit_auto: bool,
    online: Arc<AtomicBool>,
    rewrite: Rewrite,
    prepared_statements: PreparedStatements,
    dry_run: bool,
    expanded_explain: bool,
    pub_sub_channel_size: usize,
    query_parser_enabled: bool,
    connection_recovery: ConnectionRecovery,
}

/// Sharding configuration from the cluster.
#[derive(Debug, Clone, Default)]
pub struct ShardingSchema {
    /// Number of shards.
    pub shards: usize,
    /// Sharded tables.
    pub tables: ShardedTables,
    /// Scemas.
    pub schemas: ShardedSchemas,
}

impl ShardingSchema {
    pub fn tables(&self) -> &ShardedTables {
        &self.tables
    }
}

#[derive(Debug)]
pub struct ClusterShardConfig {
    pub primary: Option<PoolConfig>,
    pub replicas: Vec<PoolConfig>,
    pub role_detector: bool,
}

impl ClusterShardConfig {
    pub fn pooler_mode(&self) -> PoolerMode {
        // One of these will exist.

        if let Some(ref primary) = self.primary {
            return primary.config.pooler_mode;
        }

        self.replicas
            .first()
            .map(|replica| replica.config.pooler_mode)
            .unwrap_or_default()
    }
}

/// Cluster creation config.
#[derive(Debug)]
pub struct ClusterConfig<'a> {
    pub name: &'a str,
    pub shards: &'a [ClusterShardConfig],
    pub lb_strategy: LoadBalancingStrategy,
    pub user: &'a str,
    pub password: &'a str,
    pub pooler_mode: PoolerMode,
    pub sharded_tables: ShardedTables,
    pub replication_sharding: Option<String>,
    pub multi_tenant: &'a Option<MultiTenant>,
    pub rw_strategy: ReadWriteStrategy,
    pub rw_split: ReadWriteSplit,
    pub schema_admin: bool,
    pub cross_shard_disabled: bool,
    pub two_pc: bool,
    pub two_pc_auto: bool,
    pub sharded_schemas: ShardedSchemas,
    pub rewrite: &'a Rewrite,
    pub prepared_statements: &'a PreparedStatements,
    pub dry_run: bool,
    pub expanded_explain: bool,
    pub pub_sub_channel_size: usize,
    pub query_parser_enabled: bool,
    pub connection_recovery: ConnectionRecovery,
    pub lsn_check_interval: Duration,
}

impl<'a> ClusterConfig<'a> {
    pub(crate) fn new(
        general: &'a General,
        user: &'a User,
        shards: &'a [ClusterShardConfig],
        sharded_tables: ShardedTables,
        multi_tenant: &'a Option<MultiTenant>,
        sharded_schemas: ShardedSchemas,
        rewrite: &'a Rewrite,
    ) -> Self {
        let pooler_mode = shards
            .first()
            .map(|shard| shard.pooler_mode())
            .unwrap_or(user.pooler_mode.unwrap_or(general.pooler_mode));

        Self {
            name: &user.database,
            password: user.password(),
            user: &user.name,
            replication_sharding: user.replication_sharding.clone(),
            pooler_mode,
            lb_strategy: general.load_balancing_strategy,
            shards,
            sharded_tables,
            multi_tenant,
            rw_strategy: general.read_write_strategy,
            rw_split: general.read_write_split,
            schema_admin: user.schema_admin,
            cross_shard_disabled: user
                .cross_shard_disabled
                .unwrap_or(general.cross_shard_disabled),
            two_pc: user.two_phase_commit.unwrap_or(general.two_phase_commit),
            two_pc_auto: user
                .two_phase_commit_auto
                .unwrap_or(general.two_phase_commit_auto.unwrap_or(false)), // Disable by default.
            sharded_schemas,
            rewrite,
            prepared_statements: &general.prepared_statements,
            dry_run: general.dry_run,
            expanded_explain: general.expanded_explain,
            pub_sub_channel_size: general.pub_sub_channel_size,
            query_parser_enabled: general.query_parser_enabled,
            connection_recovery: general.connection_recovery,
            lsn_check_interval: Duration::from_millis(general.lsn_check_interval),
        }
    }
}

impl Cluster {
    /// Create new cluster of shards.
    pub fn new(config: ClusterConfig) -> Self {
        let ClusterConfig {
            name,
            shards,
            lb_strategy,
            user,
            password,
            pooler_mode,
            sharded_tables,
            replication_sharding,
            multi_tenant,
            rw_strategy,
            rw_split,
            schema_admin,
            cross_shard_disabled,
            two_pc,
            two_pc_auto,
            sharded_schemas,
            rewrite,
            prepared_statements,
            dry_run,
            expanded_explain,
            pub_sub_channel_size,
            query_parser_enabled,
            connection_recovery,
            lsn_check_interval,
        } = config;

        let identifier = Arc::new(DatabaseUser {
            user: user.to_owned(),
            database: name.to_owned(),
        });

        Self {
            identifier: identifier.clone(),
            shards: shards
                .iter()
                .enumerate()
                .map(|(number, config)| {
                    Shard::new(ShardConfig {
                        number,
                        primary: &config.primary,
                        replicas: &config.replicas,
                        lb_strategy,
                        rw_split,
                        identifier: identifier.clone(),
                        lsn_check_interval,
                        role_detector: config.role_detector,
                    })
                })
                .collect(),
            password: password.to_owned(),
            pooler_mode,
            sharded_tables,
            sharded_schemas,
            replication_sharding,
            schema: Arc::new(RwLock::new(Schema::default())),
            multi_tenant: multi_tenant.clone(),
            rw_strategy,
            schema_admin,
            stats: Arc::new(Mutex::new(MirrorStats::default())),
            cross_shard_disabled,
            two_phase_commit: two_pc && shards.len() > 1,
            two_phase_commit_auto: two_pc_auto && shards.len() > 1,
            online: Arc::new(AtomicBool::new(false)),
            rewrite: rewrite.clone(),
            prepared_statements: prepared_statements.clone(),
            dry_run,
            expanded_explain,
            pub_sub_channel_size,
            query_parser_enabled,
            connection_recovery,
        }
    }

    /// Change config to work with logical replication streaming.
    pub fn logical_stream(&self) -> Self {
        let mut cluster = self.clone();
        // Disable rewrites, we are only sending valid statements.
        cluster.rewrite.enabled = false;
        cluster
    }

    /// Get a connection to a primary of the given shard.
    pub async fn primary(&self, shard: usize, request: &Request) -> Result<Guard, Error> {
        let shard = self.shards.get(shard).ok_or(Error::NoShard(shard))?;
        shard.primary(request).await
    }

    /// Get a connection to a replica of the given shard.
    pub async fn replica(&self, shard: usize, request: &Request) -> Result<Guard, Error> {
        let shard = self.shards.get(shard).ok_or(Error::NoShard(shard))?;
        shard.replica(request).await
    }

    /// The two clusters have the same databases.
    pub(crate) fn can_move_conns_to(&self, other: &Cluster) -> bool {
        self.shards.len() == other.shards.len()
            && self
                .shards
                .iter()
                .zip(other.shards.iter())
                .all(|(a, b)| a.can_move_conns_to(b))
    }

    /// Move connections from cluster to another, saving them.
    pub(crate) fn move_conns_to(&self, other: &Cluster) {
        for (from, to) in self.shards.iter().zip(other.shards.iter()) {
            from.move_conns_to(to);
        }
    }

    /// Cancel a query executed by one of the shards.
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        for shard in &self.shards {
            shard.cancel(id).await?;
        }

        Ok(())
    }

    /// Get all shards.
    pub fn shards(&self) -> &[Shard] {
        &self.shards
    }

    /// Get the password the user should use to connect to the database.
    pub fn password(&self) -> &str {
        &self.password
    }

    /// User name.
    pub fn user(&self) -> &str {
        &self.identifier.user
    }

    /// Cluster name (database name).
    pub fn name(&self) -> &str {
        &self.identifier.database
    }

    /// Get unique cluster identifier.
    pub fn identifier(&self) -> Arc<DatabaseUser> {
        self.identifier.clone()
    }

    /// Get pooler mode.
    pub fn pooler_mode(&self) -> PoolerMode {
        self.pooler_mode
    }

    // Get sharded tables if any.
    pub fn sharded_tables(&self) -> &[ShardedTable] {
        self.sharded_tables.tables()
    }

    /// Get query rewrite config.
    pub fn rewrite(&self) -> &Rewrite {
        &self.rewrite
    }

    pub fn query_parser_enabled(&self) -> bool {
        self.query_parser_enabled
    }

    pub fn prepared_statements(&self) -> &PreparedStatements {
        &self.prepared_statements
    }

    pub fn connection_recovery(&self) -> &ConnectionRecovery {
        &self.connection_recovery
    }

    pub fn dry_run(&self) -> bool {
        self.dry_run
    }

    pub fn expanded_explain(&self) -> bool {
        self.expanded_explain
    }

    pub fn pub_sub_enabled(&self) -> bool {
        self.pub_sub_channel_size > 0
    }

    /// Find sharded column position, if the table and columns match the configuration.
    pub fn sharded_column(&self, table: &str, columns: &[&str]) -> Option<ShardedColumn> {
        self.sharded_tables.sharded_column(table, columns)
    }

    /// A cluster is read_only if zero shards have a primary.
    pub fn read_only(&self) -> bool {
        for shard in &self.shards {
            if shard.has_primary() {
                return false;
            }
        }

        true
    }

    /// This cluster is write_only if zero shards have a replica.
    pub fn write_only(&self) -> bool {
        for shard in &self.shards {
            if shard.has_replicas() {
                return false;
            }
        }

        true
    }

    /// This database/user pair is responsible for schema management.
    pub fn schema_admin(&self) -> bool {
        self.schema_admin
    }

    /// Change schema owner attribute.
    pub fn toggle_schema_admin(&mut self, owner: bool) {
        self.schema_admin = owner;
    }

    pub fn stats(&self) -> Arc<Mutex<MirrorStats>> {
        self.stats.clone()
    }

    /// We'll need the query router to figure out
    /// where a query should go.
    pub fn router_needed(&self) -> bool {
        !(self.shards().len() == 1 && (self.read_only() || self.write_only()))
    }

    /// Multi-tenant config.
    pub fn multi_tenant(&self) -> &Option<MultiTenant> {
        &self.multi_tenant
    }

    /// Get replication configuration for this cluster.
    pub fn replication_sharding_config(&self) -> Option<ReplicationConfig> {
        self.replication_sharding
            .as_ref()
            .and_then(|database| databases().replication(database))
    }

    /// Get all data required for sharding.
    pub fn sharding_schema(&self) -> ShardingSchema {
        ShardingSchema {
            shards: self.shards.len(),
            tables: self.sharded_tables.clone(),
            schemas: self.sharded_schemas.clone(),
        }
    }

    /// Update schema from primary.
    async fn update_schema(&self) -> Result<(), crate::backend::Error> {
        let mut server = self.primary(0, &Request::default()).await?;
        let schema = Schema::load(&mut server).await?;
        info!(
            "loaded {} tables from schema [{}]",
            schema.tables().len(),
            server.addr()
        );
        *self.schema.write() = schema;
        Ok(())
    }

    fn load_schema(&self) -> bool {
        self.multi_tenant.is_some()
    }

    /// Get currently loaded schema.
    pub fn schema(&self) -> Schema {
        self.schema.read().clone()
    }

    /// Read/write strategy
    pub fn read_write_strategy(&self) -> &ReadWriteStrategy {
        &self.rw_strategy
    }

    /// Cross-shard queries disabled for this cluster.
    pub fn cross_shard_disabled(&self) -> bool {
        self.cross_shard_disabled
    }

    /// Two-phase commit enabled.
    pub fn two_pc_enabled(&self) -> bool {
        self.two_phase_commit
    }

    /// Two-phase commit transactions started automatically
    /// for single-statement cross-shard writes.
    pub fn two_pc_auto_enabled(&self) -> bool {
        self.two_phase_commit_auto && self.two_pc_enabled()
    }

    /// Launch the connection pools.
    pub(crate) fn launch(&self) {
        for shard in self.shards() {
            shard.launch();
        }

        if self.load_schema() {
            let me = self.clone();
            spawn(async move {
                if let Err(err) = me.update_schema().await {
                    error!("error loading schema: {}", err);
                }
            });
        }

        self.online.store(true, Ordering::Relaxed);
    }

    /// Shutdown the connection pools.
    pub(crate) fn shutdown(&self) {
        for shard in self.shards() {
            shard.shutdown();
        }

        self.online.store(false, Ordering::Relaxed);
    }

    pub(crate) fn online(&self) -> bool {
        self.online.load(Ordering::Relaxed)
    }

    /// Execute a query on every primary in the cluster.
    pub async fn execute(
        &self,
        query: impl Into<Query> + Clone,
    ) -> Result<(), crate::backend::Error> {
        for shard in 0..self.shards.len() {
            let mut server = self.primary(shard, &Request::default()).await?;
            server.execute(query.clone()).await?;
        }

        Ok(())
    }

    /// Re-detect primary/replica roles, with shard numbers.
    pub fn redetect_roles(&self) -> Vec<Option<DetectedRoles>> {
        self.shards
            .iter()
            .map(|shard| shard.redetect_roles())
            .collect()
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use pgdog_config::OmnishardedTable;

    use crate::{
        backend::{
            pool::{Address, Config, PoolConfig, ShardConfig},
            Shard, ShardedTables,
        },
        config::{
            config, DataType, Hasher, LoadBalancingStrategy, ReadWriteSplit, ReadWriteStrategy,
            ShardedTable,
        },
    };

    use super::{Cluster, DatabaseUser};

    impl Cluster {
        pub fn new_test() -> Self {
            let config = config();
            let identifier = Arc::new(DatabaseUser {
                user: "pgdog".into(),
                database: "pgdog".into(),
            });
            let primary = &Some(PoolConfig {
                address: Address::new_test(),
                config: Config::default(),
            });
            let replicas = &[PoolConfig {
                address: Address::new_test(),
                config: Config::default(),
            }];

            let shards = (0..2)
                .into_iter()
                .map(|number| {
                    Shard::new(ShardConfig {
                        number,
                        primary,
                        replicas,
                        lb_strategy: LoadBalancingStrategy::Random,
                        rw_split: ReadWriteSplit::IncludePrimary,
                        identifier: identifier.clone(),
                        lsn_check_interval: Duration::MAX,
                        role_detector: false,
                    })
                })
                .collect::<Vec<_>>();

            Cluster {
                sharded_tables: ShardedTables::new(
                    vec![ShardedTable {
                        database: "pgdog".into(),
                        name: Some("sharded".into()),
                        column: "id".into(),
                        primary: true,
                        centroids: vec![],
                        data_type: DataType::Bigint,
                        centroids_path: None,
                        centroid_probes: 1,
                        hasher: Hasher::Postgres,
                        ..Default::default()
                    }],
                    vec![
                        OmnishardedTable {
                            name: "sharded_omni".into(),
                            sticky_routing: false,
                        },
                        OmnishardedTable {
                            name: "sharded_omni_sticky".into(),
                            sticky_routing: true,
                        },
                    ],
                ),
                shards,
                identifier,
                prepared_statements: config.config.general.prepared_statements,
                dry_run: config.config.general.dry_run,
                expanded_explain: config.config.general.expanded_explain,
                query_parser_enabled: config.config.general.query_parser_enabled,
                rewrite: config.config.rewrite.clone(),
                two_phase_commit: config.config.general.two_phase_commit,
                two_phase_commit_auto: config.config.general.two_phase_commit_auto.unwrap_or(false),
                ..Default::default()
            }
        }

        pub fn new_test_single_shard() -> Cluster {
            let mut cluster = Self::new_test();
            cluster.shards.pop();

            cluster
        }

        pub fn set_read_write_strategy(&mut self, rw_strategy: ReadWriteStrategy) {
            self.rw_strategy = rw_strategy;
        }
    }
}
