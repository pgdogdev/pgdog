//! A collection of replicas and a primary.

use crate::net::messages::BackendKeyData;

use super::{Address, Config, Error, Guard, Shard};

#[derive(Clone, Debug)]
/// Database configuration.
pub struct DatabaseConfig {
    /// Database address.
    pub(crate) address: Address,
    /// Pool settings.
    pub(crate) config: Config,
}

/// A collection of sharded replicas and primaries
/// belonging to the same database cluster.
#[derive(Clone)]
pub struct Cluster {
    shards: Vec<Shard>,
}

impl Cluster {
    /// Create new cluster of shards.
    pub fn new(shards: &[(Option<DatabaseConfig>, &[DatabaseConfig])]) -> Self {
        Self {
            shards: shards
                .iter()
                .map(|addr| Shard::new(addr.0.clone(), addr.1))
                .collect(),
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
    pub fn duplicate(&self) -> Self {
        Self {
            shards: self.shards.iter().map(|s| s.duplicate()).collect(),
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
}
