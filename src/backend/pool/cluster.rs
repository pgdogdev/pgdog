//! A collection of replicas and a primary.

use crate::net::messages::BackendKeyData;

use super::{Error, Guard, Shard};

/// A collection of sharded replicas and primaries
/// belonging to the same database cluster.
pub struct Cluster {
    shards: Vec<Shard>,
}

impl Cluster {
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
}
