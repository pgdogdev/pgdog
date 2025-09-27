//! A shard is a collection of replicas and an optional primary.

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::{interval, sleep};
use tokio::{join, select, spawn, sync::Notify};
use tracing::{debug, error};

use crate::backend::pool::replicas::ban::Ban;
use crate::backend::PubSubListener;
use crate::config::{config, LoadBalancingStrategy, ReadWriteSplit, Role};
use crate::net::messages::BackendKeyData;
use crate::net::NotificationResponse;

use super::inner::ReplicaLag;
use super::{Error, Guard, Pool, PoolConfig, Replicas, Request};

pub mod monitor;
use monitor::*;

/// Connection pools for a single database shard.
///
/// Includes a primary and replicas.
#[derive(Clone, Debug)]
pub struct Shard {
    inner: Arc<ShardInner>,
}

impl Shard {
    /// Create new shard connection pools from configuration.
    ///
    /// # Arguments
    ///
    /// * `primary`: Primary configuration, if any. Primary databases are optional.
    /// * `replica`: List of replica database configurations.
    /// * `lb_strategy`: Query load balancing strategy, e.g., random, round robin, etc.
    /// * `rw_split`: Read/write traffic splitting strategy.
    ///
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

    /// Get connection to the primary database.
    pub async fn primary(&self, request: &Request) -> Result<Guard, Error> {
        self.primary
            .as_ref()
            .ok_or(Error::NoPrimary)?
            .get_forced(request)
            .await
    }

    /// Get connection to one of the replica databases, using the configured
    /// load balancing algorithm.
    pub async fn replica(&self, request: &Request) -> Result<Guard, Error> {
        if self.replicas.is_empty() {
            self.primary
                .as_ref()
                .ok_or(Error::NoDatabases)?
                .get(request)
                .await
        } else {
            use ReadWriteSplit::*;

            let primary_reads = match self.rw_split {
                IncludePrimary => true,
                ExcludePrimary => false,
            };

            self.replicas.get(request, primary_reads).await
        }
    }

    /// Get connection to primary if configured, otherwise replica.
    pub async fn primary_or_replica(&self, request: &Request) -> Result<Guard, Error> {
        if self.primary.is_some() {
            self.primary(request).await
        } else {
            self.replica(request).await
        }
    }

    /// Move connections from this shard to another shard, preserving them.
    ///
    /// This is done during configuration reloading, if no significant changes are made to
    /// the configuration.
    pub fn move_conns_to(&self, destination: &Shard) {
        if let Some(ref primary) = self.primary {
            if let Some(ref other) = destination.primary {
                primary.move_conns_to(other);
            }
        }

        self.replicas.move_conns_to(&destination.replicas);
    }

    /// Checks if the connection pools from this shard are compatible
    /// with the other shard. If yes, they can be moved without closing them.
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

    /// Listen for notifications on channel.
    pub async fn listen(
        &self,
        channel: &str,
    ) -> Result<broadcast::Receiver<NotificationResponse>, Error> {
        if let Some(ref listener) = self.pub_sub {
            listener.listen(channel).await
        } else {
            Err(Error::PubSubDisabled)
        }
    }

    /// Notify channel with optional payload (payload can be empty string).
    pub async fn notify(&self, channel: &str, payload: &str) -> Result<(), Error> {
        if let Some(ref listener) = self.pub_sub {
            listener.notify(channel, payload).await
        } else {
            Err(Error::PubSubDisabled)
        }
    }

    /// Clone pools but keep them independent.
    pub fn duplicate(&self) -> Self {
        let primary = self
            .inner
            .primary
            .as_ref()
            .map(|primary| primary.duplicate());
        let pub_sub = if self.pub_sub.is_some() {
            primary.as_ref().map(PubSubListener::new)
        } else {
            None
        };

        Self {
            inner: Arc::new(ShardInner {
                primary,
                pub_sub,
                replicas: self.inner.replicas.duplicate(),
                rw_split: self.inner.rw_split,
                comms: ShardComms::default(), // Create new comms instead of duplicating
            }),
        }
    }

    /// Bring every pool online.
    pub fn launch(&self) {
        self.pools().iter().for_each(|pool| pool.launch());
        ShardMonitor::run(self);
        if let Some(ref listener) = self.pub_sub {
            listener.launch();
        }
    }

    /// Returns true if the shard has a primary database.
    pub fn has_primary(&self) -> bool {
        self.primary.is_some()
    }

    /// Returns true if the shard has any replica databases.
    pub fn has_replicas(&self) -> bool {
        !self.replicas.is_empty()
    }

    /// Request a query to be cancelled on any of the servers in the connection pools
    /// in this shard.
    ///
    /// # Arguments
    ///
    /// * `id`: Client unique identifier. Clients can execute one query at a time.
    ///
    /// If these connection pools aren't running the query sent by this client, this is a no-op.
    ///
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        if let Some(ref primary) = self.primary {
            primary.cancel(id).await?;
        }
        self.replicas.cancel(id).await?;

        Ok(())
    }

    /// Get all connection pools.
    pub fn pools(&self) -> Vec<Pool> {
        self.pools_with_roles()
            .into_iter()
            .map(|(_, pool)| pool)
            .collect()
    }

    /// Get all connection pools along with their roles (i.e., primary or replica).
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

    /// Get all connection pools with bans and their role in the shard.
    pub fn pools_with_roles_and_bans(&self) -> Vec<(Role, Ban, Pool)> {
        self.replicas.pools_with_roles_and_bans()
    }

    /// Shutdown every pool.
    pub fn shutdown(&self) {
        self.comms.shutdown.notify_waiters();
        self.pools().iter().for_each(|pool| pool.shutdown());
        if let Some(ref listener) = self.pub_sub {
            listener.shutdown();
        }
    }

    fn comms(&self) -> &ShardComms {
        &self.comms
    }
}

impl Deref for Shard {
    type Target = ShardInner;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Shard connection pools
/// and internal state.
#[derive(Default, Debug)]
pub struct ShardInner {
    primary: Option<Pool>,
    replicas: Replicas,
    rw_split: ReadWriteSplit,
    comms: ShardComms,
    pub_sub: Option<PubSubListener>,
}

impl ShardInner {
    fn new(
        primary: &Option<PoolConfig>,
        replicas: &[PoolConfig],
        lb_strategy: LoadBalancingStrategy,
        rw_split: ReadWriteSplit,
    ) -> Self {
        let primary = primary.as_ref().map(Pool::new);
        let replicas = Replicas::new(&primary, replicas, lb_strategy);
        let comms = ShardComms {
            shutdown: Notify::new(),
        };
        let pub_sub = if config().pub_sub_enabled() {
            primary.as_ref().map(PubSubListener::new)
        } else {
            None
        };

        Self {
            primary,
            replicas,
            rw_split,
            comms,
            pub_sub,
        }
    }
}

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

        let shard = Shard::new(
            primary,
            replicas,
            LoadBalancingStrategy::Random,
            ReadWriteSplit::ExcludePrimary,
        );
        shard.launch();

        for _ in 0..25 {
            let replica_id = shard.replicas.replica_pools[0].id();

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

        let shard = Shard::new(
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
