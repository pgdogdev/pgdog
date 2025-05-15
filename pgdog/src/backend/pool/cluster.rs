//! A collection of replicas and a primary.

use parking_lot::RwLock;
use std::sync::Arc;
use tokio::spawn;
use tracing::{error, info};

use crate::{
    backend::{
        databases::databases,
        replication::{ReplicationConfig, ShardedColumn},
        Schema, ShardedTables,
    },
    config::{General, MultiTenant, PoolerMode, ReadWriteStrategy, ShardedTable, User},
    net::messages::BackendKeyData,
};

use super::{Address, Config, Error, Guard, Request, Shard};
use crate::config::LoadBalancingStrategy;

#[derive(Clone, Debug)]
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
    name: String,
    shards: Vec<Shard>,
    user: String,
    password: String,
    pooler_mode: PoolerMode,
    sharded_tables: ShardedTables,
    replication_sharding: Option<String>,
    mirror_of: Option<String>,
    schema: Arc<RwLock<Schema>>,
    multi_tenant: Option<MultiTenant>,
    rw_strategy: ReadWriteStrategy,
}

/// Sharding configuration from the cluster.
#[derive(Debug, Clone, Default)]
pub struct ShardingSchema {
    /// Number of shards.
    pub shards: usize,
    /// Sharded tables.
    pub tables: ShardedTables,
}

pub struct ClusterShardConfig {
    pub primary: Option<PoolConfig>,
    pub replicas: Vec<PoolConfig>,
}

/// Cluster creation config.
pub struct ClusterConfig<'a> {
    pub name: &'a str,
    pub shards: &'a [ClusterShardConfig],
    pub lb_strategy: LoadBalancingStrategy,
    pub user: &'a str,
    pub password: &'a str,
    pub pooler_mode: PoolerMode,
    pub sharded_tables: ShardedTables,
    pub replication_sharding: Option<String>,
    pub mirror_of: Option<&'a str>,
    pub multi_tenant: &'a Option<MultiTenant>,
    pub rw_strategy: ReadWriteStrategy,
}

impl<'a> ClusterConfig<'a> {
    pub(crate) fn new(
        general: &'a General,
        user: &'a User,
        shards: &'a [ClusterShardConfig],
        sharded_tables: ShardedTables,
        mirror_of: Option<&'a str>,
        multi_tenant: &'a Option<MultiTenant>,
    ) -> Self {
        Self {
            name: &user.database,
            password: user.password(),
            user: &user.name,
            replication_sharding: user.replication_sharding.clone(),
            pooler_mode: user.pooler_mode.unwrap_or(general.pooler_mode),
            lb_strategy: general.load_balancing_strategy,
            shards,
            sharded_tables,
            mirror_of,
            multi_tenant,
            rw_strategy: general.read_write_strategy,
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
            mirror_of,
            multi_tenant,
            rw_strategy,
        } = config;

        Self {
            shards: shards
                .iter()
                .map(|config| Shard::new(&config.primary, &config.replicas, lb_strategy))
                .collect(),
            name: name.to_owned(),
            password: password.to_owned(),
            user: user.to_owned(),
            pooler_mode,
            sharded_tables,
            replication_sharding,
            mirror_of: mirror_of.map(|s| s.to_owned()),
            schema: Arc::new(RwLock::new(Schema::default())),
            multi_tenant: multi_tenant.clone(),
            rw_strategy,
        }
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

    /// Create new identical cluster connection pool.
    ///
    /// This will allocate new server connections. Use when reloading configuration
    /// and you expect to drop the current Cluster entirely.
    pub fn duplicate(&self) -> Self {
        Self {
            shards: self.shards.iter().map(|s| s.duplicate()).collect(),
            name: self.name.clone(),
            user: self.user.clone(),
            password: self.password.clone(),
            pooler_mode: self.pooler_mode,
            sharded_tables: self.sharded_tables.clone(),
            replication_sharding: self.replication_sharding.clone(),
            mirror_of: self.mirror_of.clone(),
            schema: self.schema.clone(),
            multi_tenant: self.multi_tenant.clone(),
            rw_strategy: self.rw_strategy,
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

    /// Mirrors getter.
    pub fn mirror_of(&self) -> Option<&str> {
        self.mirror_of.as_deref()
    }

    /// Get the password the user should use to connect to the database.
    pub fn password(&self) -> &str {
        &self.password
    }

    /// User name.
    pub fn user(&self) -> &str {
        &self.user
    }

    /// Cluster name (database name).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get pooler mode.
    pub fn pooler_mode(&self) -> PoolerMode {
        self.pooler_mode
    }

    // Get sharded tables if any.
    pub fn sharded_tables(&self) -> &[ShardedTable] {
        self.sharded_tables.tables()
    }

    /// Find sharded column position, if the table and columns match the configuration.
    pub fn sharded_column(&self, table: &str, columns: &[&str]) -> Option<ShardedColumn> {
        self.sharded_tables.sharded_column(table, columns)
    }

    /// This cluster is read only (no primaries).
    pub fn read_only(&self) -> bool {
        for shard in &self.shards {
            if shard.primary.is_some() {
                return false;
            }
        }

        true
    }

    ///  This cluster is write only (no replicas).
    pub fn write_only(&self) -> bool {
        for shard in &self.shards {
            if !shard.replicas.is_empty() {
                return false;
            }
        }

        true
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
    }

    /// Shutdown the connection pools.
    pub(crate) fn shutdown(&self) {
        for shard in self.shards() {
            shard.shutdown();
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        backend::{Shard, ShardedTables},
        config::{DataType, ReadWriteStrategy, ShardedTable},
    };

    use super::Cluster;

    impl Cluster {
        pub fn new_test() -> Self {
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
                    }],
                    vec!["sharded_omni".into()],
                    false,
                ),
                shards: vec![Shard::default(), Shard::default()],
                ..Default::default()
            }
        }

        pub fn set_read_write_strategy(&mut self, rw_strategy: ReadWriteStrategy) {
            self.rw_strategy = rw_strategy;
        }
    }
}
