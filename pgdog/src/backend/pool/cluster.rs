//! A collection of replicas and a primary.

use crate::{
    backend::{
        databases::databases,
        replication::{ReplicationConfig, ShardedColumn},
        ShardedTables,
    },
    config::{General, PoolerMode, ShardedTable, TenantTable, User},
    net::messages::BackendKeyData,
};

use super::{Address, Config, Error, Guard, Request, Shard};
use crate::config::LoadBalancingStrategy;

use std::ffi::CString;

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
    tenant_tables: Vec<TenantTable>,
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
    pub tenant_tables: &'a [TenantTable],
}

impl<'a> ClusterConfig<'a> {
    pub(crate) fn new(
        general: &'a General,
        user: &'a User,
        shards: &'a [ClusterShardConfig],
        sharded_tables: ShardedTables,
        mirror_of: Option<&'a str>,
        tenant_tables: &'a [TenantTable],
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
            tenant_tables,
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
            tenant_tables,
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
            tenant_tables: tenant_tables.to_vec(),
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
            tenant_tables: self.tenant_tables.clone(),
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

    /// Plugin input.
    ///
    /// # Safety
    ///
    /// This allocates, so make sure to call `Config::drop` when you're done.
    ///
    pub unsafe fn plugin_config(&self) -> Result<pgdog_plugin::bindings::Config, Error> {
        use pgdog_plugin::bindings::{Config, DatabaseConfig, Role_PRIMARY, Role_REPLICA};
        let mut databases: Vec<DatabaseConfig> = vec![];
        let name = CString::new(self.name.as_str()).map_err(|_| Error::NullBytes)?;

        for (index, shard) in self.shards.iter().enumerate() {
            if let Some(ref primary) = shard.primary {
                // Ignore hosts with null bytes.
                let host = if let Ok(host) = CString::new(primary.addr().host.as_str()) {
                    host
                } else {
                    continue;
                };
                databases.push(DatabaseConfig::new(
                    host,
                    primary.addr().port,
                    Role_PRIMARY,
                    index,
                ));
            }

            for replica in shard.replicas.pools() {
                // Ignore hosts with null bytes.
                let host = if let Ok(host) = CString::new(replica.addr().host.as_str()) {
                    host
                } else {
                    continue;
                };
                databases.push(DatabaseConfig::new(
                    host,
                    replica.addr().port,
                    Role_REPLICA,
                    index,
                ));
            }
        }

        Ok(Config::new(name, &databases, self.shards.len()))
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

    pub fn tenant_tables(&self) -> &[TenantTable] {
        &self.tenant_tables
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

    /// Launch the connection pools.
    pub(crate) fn launch(&self) {
        for shard in self.shards() {
            shard.launch();
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
        config::{DataType, ShardedTable},
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
    }
}
