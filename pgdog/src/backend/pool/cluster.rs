//! A collection of replicas and a primary.

use crate::{
    config::{PoolerMode, ShardedTable},
    net::messages::BackendKeyData,
};

use super::{Address, Config, Error, Guard, Shard};
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
#[derive(Clone)]
pub struct Cluster {
    name: String,
    shards: Vec<Shard>,
    password: String,
    pooler_mode: PoolerMode,
    sharded_tables: Vec<ShardedTable>,
}

impl Cluster {
    /// Create new cluster of shards.
    pub fn new(
        name: &str,
        shards: &[(Option<PoolConfig>, Vec<PoolConfig>)],
        lb_strategy: LoadBalancingStrategy,
        password: &str,
        pooler_mode: PoolerMode,
        sharded_tables: Vec<ShardedTable>,
    ) -> Self {
        Self {
            shards: shards
                .iter()
                .map(|addr| Shard::new(addr.0.clone(), &addr.1, lb_strategy))
                .collect(),
            name: name.to_owned(),
            password: password.to_owned(),
            pooler_mode,
            sharded_tables,
        }
    }

    /// Get a connection to a primary of the given shard.
    pub async fn primary(&self, shard: usize, id: &BackendKeyData) -> Result<Guard, Error> {
        let shard = self.shards.get(shard).ok_or(Error::NoShard(shard))?;
        shard.primary(id).await
    }

    /// Get a connection to a replica of the given shard.
    pub async fn replica(&self, shard: usize, id: &BackendKeyData) -> Result<Guard, Error> {
        let shard = self.shards.get(shard).ok_or(Error::NoShard(shard))?;
        shard.replica(id).await
    }

    /// Create new identical cluster connection pool.
    ///
    /// This will allocate new server connections. Use when reloading configuration
    /// and you expect to drop the current Cluster entirely.
    pub fn duplicate(&self) -> Self {
        Self {
            shards: self.shards.iter().map(|s| s.duplicate()).collect(),
            name: self.name.clone(),
            password: self.password.clone(),
            pooler_mode: self.pooler_mode,
            sharded_tables: self.sharded_tables.clone(),
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

    /// Get pooler mode.
    pub fn pooler_mode(&self) -> PoolerMode {
        self.pooler_mode
    }

    // Get sharded tables if any.
    pub fn shaded_tables(&self) -> &[ShardedTable] {
        &self.sharded_tables
    }

    /// Find sharded column position, if the table and columns match the configuration.
    pub fn sharded_column(&self, table: &str, columns: &[&str]) -> Option<usize> {
        let table = self.sharded_tables.iter().find(|sharded_table| {
            sharded_table
                .name
                .as_ref()
                .map(|name| name == table)
                .unwrap_or(true)
                && columns.contains(&sharded_table.column.as_str())
        });

        table
            .map(|t| columns.iter().position(|c| *c == &t.column))
            .flatten()
    }
}

// pub struct PluginConfig {
//     config: pgdog_plugin::bindings::Config,
// }

// impl Drop for PluginConfig {
//     fn drop(&mut self) {
//         unsafe {
//             self.config.deallocate();
//         }
//     }
// }
