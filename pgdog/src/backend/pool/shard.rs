//! A shard is a collection of replicas and a primary.

use crate::net::messages::BackendKeyData;

use super::{Address, Config, Error, Guard, Pool, Replicas};

/// Primary and replicas.
#[derive(Clone)]
pub struct Shard {
    primary: Pool,
    replicas: Replicas,
}

impl Shard {
    /// Create new shard connection pool.
    pub fn new(primary: &Address, replicas: &[&Address]) -> Self {
        let primary = Pool::new(primary, Config::default_primary());
        let replicas = Replicas::new(replicas);

        Self { primary, replicas }
    }

    /// Get a connection to the shard primary database.
    pub async fn primary(&self, id: &BackendKeyData) -> Result<Guard, Error> {
        self.primary.get(id).await
    }

    /// Get a connection to a shard replica, if any.
    pub async fn replica(&self, id: &BackendKeyData) -> Result<Guard, Error> {
        if self.replicas.is_empty() {
            self.primary.get(id).await
        } else {
            self.replicas.get(id, &self.primary).await
        }
    }

    /// Create new identical connection pool.
    pub fn duplicate(&self) -> Self {
        Self {
            primary: self.primary.duplicate(),
            replicas: self.replicas.duplicate(),
        }
    }

    /// Cancel a query if one is running.
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        self.primary.cancel(id).await?;
        self.replicas.cancel(id).await?;

        Ok(())
    }

    /// Get all pools. Used for administrative tasks.
    pub fn pools(&self) -> Vec<Pool> {
        let mut pools = vec![self.primary.clone()];
        pools.extend(self.replicas.pools().to_vec());

        pools
    }
}
