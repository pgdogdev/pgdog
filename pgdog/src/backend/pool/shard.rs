//! A shard is a collection of replicas and a primary.

use crate::{config::LoadBalancingStrategy, net::messages::BackendKeyData};

use super::{Error, Guard, Pool, PoolConfig, Replicas};

/// Primary and replicas.
#[derive(Clone)]
pub struct Shard {
    pub(super) primary: Option<Pool>,
    pub(super) replicas: Replicas,
}

impl Shard {
    /// Create new shard connection pool.
    pub fn new(
        primary: Option<PoolConfig>,
        replicas: &[PoolConfig],
        lb_strategy: LoadBalancingStrategy,
    ) -> Self {
        let primary = primary.map(Pool::new);
        let replicas = Replicas::new(replicas, lb_strategy);

        Self { primary, replicas }
    }

    /// Get a connection to the shard primary database.
    pub async fn primary(&self, id: &BackendKeyData) -> Result<Guard, Error> {
        self.primary.as_ref().ok_or(Error::NoPrimary)?.get(id).await
    }

    /// Get a connection to a shard replica, if any.
    pub async fn replica(&self, id: &BackendKeyData) -> Result<Guard, Error> {
        if self.replicas.is_empty() {
            self.primary
                .as_ref()
                .ok_or(Error::NoDatabases)?
                .get(id)
                .await
        } else {
            self.replicas.get(id, &self.primary).await
        }
    }

    /// Create new identical connection pool.
    pub fn duplicate(&self) -> Self {
        Self {
            primary: self.primary.as_ref().map(|primary| primary.duplicate()),
            replicas: self.replicas.duplicate(),
        }
    }

    /// Cancel a query if one is running.
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        if let Some(ref primary) = self.primary {
            primary.cancel(id).await?;
        }
        self.replicas.cancel(id).await?;

        Ok(())
    }

    /// Get all pools. Used for administrative tasks.
    pub fn pools(&self) -> Vec<Pool> {
        let mut pools = vec![];
        if let Some(primary) = self.primary.clone() {
            pools.push(primary);
        }
        pools.extend(self.replicas.pools().to_vec());

        pools
    }
}
