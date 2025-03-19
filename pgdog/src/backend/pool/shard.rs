//! A shard is a collection of replicas and a primary.

use crate::{
    config::LoadBalancingStrategy,
    net::messages::{BackendKeyData, Vector},
};

use super::{Error, Guard, Pool, PoolConfig, Replicas, Request};

/// Primary and replicas.
#[derive(Clone, Default, Debug)]
pub struct Shard {
    pub(super) primary: Option<Pool>,
    pub(super) replicas: Replicas,
    pub(super) centroid: Option<Vector>,
}

impl Shard {
    /// Create new shard connection pool.
    pub fn new(
        primary: &Option<PoolConfig>,
        replicas: &[PoolConfig],
        lb_strategy: LoadBalancingStrategy,
        centroid: Option<Vector>,
    ) -> Self {
        let primary = primary.as_ref().map(Pool::new);
        let replicas = Replicas::new(replicas, lb_strategy);

        Self {
            primary,
            replicas,
            centroid,
        }
    }

    /// Get a connection to the shard primary database.
    pub async fn primary(&self, request: &Request) -> Result<Guard, Error> {
        self.primary
            .as_ref()
            .ok_or(Error::NoPrimary)?
            .get(request)
            .await
    }

    /// Get a connection to a shard replica, if any.
    pub async fn replica(&self, request: &Request) -> Result<Guard, Error> {
        if self.replicas.is_empty() {
            self.primary
                .as_ref()
                .ok_or(Error::NoDatabases)?
                .get(request)
                .await
        } else {
            self.replicas.get(request, &self.primary).await
        }
    }

    /// Create new identical connection pool.
    pub fn duplicate(&self) -> Self {
        Self {
            primary: self.primary.as_ref().map(|primary| primary.duplicate()),
            replicas: self.replicas.duplicate(),
            centroid: self.centroid.clone(),
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

    /// Launch the shard, bringing all pools online.
    pub fn launch(&self) {
        self.pools().iter().for_each(|pool| pool.launch());
    }

    /// Shutdown all pools, taking the shard offline.
    pub fn shutdown(&self) {
        self.pools().iter().for_each(|pool| pool.shutdown());
    }

    /// Get the shard vector centroid.
    pub fn centroid(&self) -> &Option<Vector> {
        &self.centroid
    }
}
