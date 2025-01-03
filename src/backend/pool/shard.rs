//! A shard is a collection of replicas and a primary.

use crate::net::messages::BackendKeyData;

use super::{Error, Guard, Pool, Replicas};

/// Primary and replicas.
pub struct Shard {
    primary: Pool,
    replicas: Replicas,
}

impl Shard {
    /// Create new shard connection pool.
    pub fn new() -> Self {
        todo!()
    }

    /// Get a connection to the shard primary database.
    pub async fn primary(&self, id: &BackendKeyData) -> Result<Guard, Error> {
        self.primary.get(id).await
    }

    /// Get a connection to a shard replica.
    pub async fn replica(&self, id: &BackendKeyData) -> Result<Guard, Error> {
        self.replicas.get(id).await
    }

    /// Create new identical connection pool.
    pub fn duplicate(&self) -> Self {
        Self {
            primary: self.primary.duplicate(),
            replicas: self.replicas.duplicate(),
        }
    }
}
