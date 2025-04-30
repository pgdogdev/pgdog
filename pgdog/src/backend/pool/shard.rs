//! A shard is a collection of replicas and a primary.

use crate::{
    config::{LoadBalancingStrategy, Role},
    net::messages::BackendKeyData,
};

use super::{Error, Guard, Pool, PoolConfig, Replicas, Request};

/// Primary and replicas.
#[derive(Clone, Default, Debug)]
pub struct Shard {
    pub(super) primary: Option<Pool>,
    pub(super) replicas: Replicas,
}

impl Shard {
    /// Create new shard connection pool.
    pub fn new(
        primary: &Option<PoolConfig>,
        replicas: &[PoolConfig],
        lb_strategy: LoadBalancingStrategy,
    ) -> Self {
        let primary = primary.as_ref().map(Pool::new);
        let replicas = Replicas::new(replicas, lb_strategy);

        Self { primary, replicas }
    }

    /// Get a connection to the shard primary database.
    pub async fn primary(&self, request: &Request) -> Result<Guard, Error> {
        self.primary
            .as_ref()
            .ok_or(Error::NoPrimary)?
            .get_forced(request)
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

    /// Move pool connections from self to destination.
    /// This shuts down my pool.
    pub fn move_conns_to(&self, destination: &Shard) {
        if let Some(ref primary) = self.primary {
            if let Some(ref other) = destination.primary {
                primary.move_conns_to(other);
            }
        }

        self.replicas.move_conns_to(&destination.replicas);
    }

    /// The two shards have the same databases.
    pub(crate) fn can_move_conns_to(&self, other: &Shard) -> bool {
        if let Some(ref primary) = self.primary {
            if let Some(ref other) = other.primary {
                if !primary.can_move_conns_to(other) {
                    return false;
                }
            } else {
                return false;
            }
        }

        self.replicas.can_move_conns_to(&other.replicas)
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
        self.pools_with_roles()
            .into_iter()
            .map(|(_, pool)| pool)
            .collect()
    }

    pub fn pools_with_roles(&self) -> Vec<(Role, Pool)> {
        let mut pools = vec![];
        if let Some(primary) = self.primary.clone() {
            pools.push((Role::Primary, primary));
        }
        pools.extend(
            self.replicas
                .pools()
                .iter()
                .map(|p| (Role::Replica, p.clone())),
        );

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
}
