//! A shard is a collection of replicas and an optional primary.

use std::sync::Arc;

use crate::config::{LoadBalancingStrategy, ReadWriteSplit, Role};
use crate::net::messages::BackendKeyData;

use super::{Error, Guard, Pool, PoolConfig, Replicas, Request};

// -------------------------------------------------------------------------------------------------
// ----- Public Interface --------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Shard {
    inner: Arc<ShardInner>,
}

impl Shard {
    /// Build a new shard.
    pub fn new(
        primary: &Option<PoolConfig>,
        replicas: &[PoolConfig],
        lb_strategy: LoadBalancingStrategy,
        rw_split: ReadWriteSplit,
    ) -> Self {
        Self {
            inner: Arc::new(ShardInner::new(primary, replicas, lb_strategy, rw_split)),
        }
    }

    /// Get a connection to the shard primary database.
    pub async fn primary(&self, request: &Request) -> Result<Guard, Error> {
        self.inner.primary(request).await
    }

    /// Get a connection to a shard replica (or primary, if allowed).
    pub async fn replica(&self, request: &Request) -> Result<Guard, Error> {
        self.inner.replica(request).await
    }

    /// Move pool connections from this shard to `destination`.
    pub fn move_conns_to(&self, destination: &Shard) {
        self.inner.move_conns_to(&destination.inner);
    }

    /// Check if pools can be moved to `other`.
    pub(crate) fn can_move_conns_to(&self, other: &Shard) -> bool {
        self.inner.can_move_conns_to(&other.inner)
    }

    /// Clone pools but keep them independent.
    pub fn duplicate(&self) -> Self {
        Self {
            inner: Arc::new(self.inner.duplicate()),
        }
    }

    /// Cancel a running query.
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        self.inner.cancel(id).await
    }

    /// All pools in this shard.
    pub fn pools(&self) -> Vec<Pool> {
        self.inner.pools()
    }

    /// All pools with their roles.
    pub fn pools_with_roles(&self) -> Vec<(Role, Pool)> {
        self.inner.pools_with_roles()
    }

    /// Bring every pool online.
    pub fn launch(&self) {
        self.inner.launch();
    }

    /// Shut everything down.
    pub fn shutdown(&self) {
        self.inner.shutdown();
    }

    pub fn is_write_only(&self) -> bool {
        self.inner.replicas.is_empty()
    }

    pub fn is_read_only(&self) -> bool {
        self.inner.primary.is_none()
    }
}

// -------------------------------------------------------------------------------------------------
// ----- Private Implementation --------------------------------------------------------------------

#[derive(Default, Debug)]
struct ShardInner {
    primary: Option<Pool>,
    replicas: Replicas,
    rw_split: ReadWriteSplit,
}

impl ShardInner {
    fn new(
        primary: &Option<PoolConfig>,
        replicas: &[PoolConfig],
        lb_strategy: LoadBalancingStrategy,
        rw_split: ReadWriteSplit,
    ) -> Self {
        let primary = primary.as_ref().map(Pool::new);
        let replicas = Replicas::new(replicas, lb_strategy);

        Self {
            primary,
            replicas,
            rw_split,
        }
    }

    async fn primary(&self, request: &Request) -> Result<Guard, Error> {
        self.primary
            .as_ref()
            .ok_or(Error::NoPrimary)?
            .get_forced(request)
            .await
    }

    async fn replica(&self, request: &Request) -> Result<Guard, Error> {
        if self.replicas.is_empty() {
            self.primary
                .as_ref()
                .ok_or(Error::NoDatabases)?
                .get(request)
                .await
        } else {
            use ReadWriteSplit::*;
            let primary = match self.rw_split {
                IncludePrimary => &self.primary,
                ExcludePrimary => &None,
            };
            self.replicas.get(request, primary).await
        }
    }

    fn move_conns_to(&self, destination: &ShardInner) {
        if let (Some(src), Some(dst)) = (self.primary.as_ref(), destination.primary.as_ref()) {
            src.move_conns_to(dst);
        }
        self.replicas.move_conns_to(&destination.replicas);
    }

    fn can_move_conns_to(&self, other: &ShardInner) -> bool {
        if let (Some(a), Some(b)) = (self.primary.as_ref(), other.primary.as_ref()) {
            if !a.can_move_conns_to(b) {
                return false;
            }
        } else if self.primary.is_some() || other.primary.is_some() {
            return false;
        }
        self.replicas.can_move_conns_to(&other.replicas)
    }

    fn duplicate(&self) -> Self {
        Self {
            primary: self.primary.as_ref().map(|p| p.duplicate()),
            replicas: self.replicas.duplicate(),
            rw_split: self.rw_split,
        }
    }

    async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        if let Some(ref primary) = self.primary {
            primary.cancel(id).await?;
        }
        self.replicas.cancel(id).await?;
        Ok(())
    }

    fn pools(&self) -> Vec<Pool> {
        self.pools_with_roles()
            .into_iter()
            .map(|(_, p)| p)
            .collect()
    }

    fn pools_with_roles(&self) -> Vec<(Role, Pool)> {
        let mut pools = Vec::new();
        if let Some(p) = self.primary.clone() {
            pools.push((Role::Primary, p));
        }
        pools.extend(
            self.replicas
                .pools()
                .iter()
                .cloned()
                .map(|p| (Role::Replica, p)),
        );
        pools
    }

    fn launch(&self) {
        self.pools().iter().for_each(Pool::launch);
    }

    fn shutdown(&self) {
        self.pools().iter().for_each(Pool::shutdown);
    }
}

// -------------------------------------------------------------------------------------------------
// ----- Monitoring --------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------------------
// ----- Tests -------------------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use crate::backend::pool::{Address, Config};

    use super::*;

    #[tokio::test]
    async fn test_exclude_primary() {
        crate::logger();

        let primary = &Some(PoolConfig {
            address: Address::new_test(),
            config: Config::default(),
        });

        let replicas = &[PoolConfig {
            address: Address::new_test(),
            config: Config::default(),
        }];

        let shard = ShardInner::new(
            primary,
            replicas,
            LoadBalancingStrategy::Random,
            ReadWriteSplit::ExcludePrimary,
        );
        shard.launch();

        for _ in 0..25 {
            let replica_id = shard.replicas.pools[0].id();

            let conn = shard.replica(&Request::default()).await.unwrap();
            assert_eq!(conn.pool.id(), replica_id);
        }

        shard.shutdown();
    }

    #[tokio::test]
    async fn test_include_primary() {
        crate::logger();

        let primary = &Some(PoolConfig {
            address: Address::new_test(),
            config: Config::default(),
        });

        let replicas = &[PoolConfig {
            address: Address::new_test(),
            config: Config::default(),
        }];

        let shard = ShardInner::new(
            primary,
            replicas,
            LoadBalancingStrategy::Random,
            ReadWriteSplit::IncludePrimary,
        );
        shard.launch();
        let mut ids = BTreeSet::new();

        for _ in 0..25 {
            let conn = shard.replica(&Request::default()).await.unwrap();
            ids.insert(conn.pool.id());
        }

        shard.shutdown();

        assert_eq!(ids.len(), 2);
    }
}

// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------
