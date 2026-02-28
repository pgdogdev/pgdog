//! A shard is a collection of replicas and an optional primary.

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, OnceCell};
use tokio::{select, spawn, sync::Notify};
use tracing::{debug, info};

use crate::backend::databases::User;
use crate::backend::pool::lb::ban::Ban;
use crate::backend::PubSubListener;
use crate::backend::Schema;
use crate::config::{config, LoadBalancingStrategy, ReadWriteSplit, Role};
use crate::net::messages::BackendKeyData;
use crate::net::{NotificationResponse, Parameters};

use super::{Error, Guard, LoadBalancer, Pool, PoolConfig, Request};

pub mod monitor;
pub mod role_detector;

use monitor::*;
use role_detector::*;

pub(super) struct ShardConfig<'a> {
    /// Shard number.
    pub(super) number: usize,
    /// Shard primary database, if any.
    pub(super) primary: &'a Option<PoolConfig>,
    /// Shard replica databases.
    pub(super) replicas: &'a [PoolConfig],
    /// Load balancing strategy for replicas.
    pub(super) lb_strategy: LoadBalancingStrategy,
    /// Primary/replica read/write split strategy.
    pub(super) rw_split: ReadWriteSplit,
    /// Cluster identifier (user/password).
    pub(super) identifier: Arc<User>,
    /// LSN check interval
    pub(super) lsn_check_interval: Duration,
}

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
    pub(super) fn new(config: ShardConfig<'_>) -> Self {
        Self {
            inner: Arc::new(ShardInner::new(config)),
        }
    }

    /// Get connection to the primary database.
    pub async fn primary(&self, request: &Request) -> Result<Guard, Error> {
        self.lb
            .primary()
            .ok_or(Error::NoPrimary)?
            .get(request)
            .await
    }

    /// Get connection to one of the replica databases, using the configured
    /// load balancing algorithm.
    pub async fn replica(&self, request: &Request) -> Result<Guard, Error> {
        self.lb.get(request).await
    }

    /// Get connection to primary if configured, otherwise replica.
    pub async fn primary_or_replica(&self, request: &Request) -> Result<Guard, Error> {
        if let Ok(primary) = self.primary(request).await {
            Ok(primary)
        } else {
            self.replica(request).await
        }
    }

    /// Move connections from this shard to another shard, preserving them.
    ///
    /// This is done during configuration reloading, if no significant changes are made to
    /// the configuration.
    pub fn move_conns_to(&self, destination: &Shard) -> Result<(), Error> {
        self.lb.move_conns_to(&destination.lb)?;

        Ok(())
    }

    /// Checks if the connection pools from this shard are compatible
    /// with the other shard. If yes, they can be moved without closing them.
    pub(crate) fn can_move_conns_to(&self, other: &Shard) -> bool {
        self.lb.can_move_conns_to(&other.lb)
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

    /// Load schema from the shard's primary.
    pub async fn update_schema(&self) -> Result<(), crate::backend::Error> {
        let mut server = self.primary_or_replica(&Request::default()).await?;
        let schema = Schema::load(&mut server).await?;
        info!(
            "loaded schema for {} tables on shard {} [{}]",
            schema.tables().len(),
            self.number(),
            server.addr()
        );
        let _ = self.schema.set(schema);
        Ok(())
    }

    /// Bring every pool online.
    pub fn launch(&self) {
        self.lb.launch();
        ShardMonitor::run(self);
        if let Some(ref listener) = self.pub_sub {
            listener.launch();
        }
    }

    /// Pause every pool in this shard.
    pub fn pause(&self) {
        self.lb.pause();
    }

    /// Resume every pool in this shard.
    pub fn resume(&self) {
        self.lb.resume();
    }

    /// Returns true if the shard has a primary database.
    pub fn has_primary(&self) -> bool {
        self.lb.primary().is_some()
    }

    /// Returns true if the shard has any replica databases.
    pub fn has_replicas(&self) -> bool {
        self.lb.has_replicas()
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
        self.lb.cancel(id).await?;

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

        pools.extend(
            self.lb
                .targets
                .iter()
                .map(|target| (target.role(), target.pool.clone())),
        );

        pools
    }

    /// Get all connection pools with bans and their role in the shard.
    pub fn pools_with_roles_and_bans(&self) -> Vec<(Role, Ban, Pool)> {
        self.lb.pools_with_roles_and_bans()
    }

    /// Shutdown every pool and maintenance task in this shard.
    pub fn shutdown(&self) {
        self.comms.shutdown.notify_waiters();
        if let Some(ref listener) = self.pub_sub {
            listener.shutdown();
        }
        self.lb.shutdown();
    }

    fn comms(&self) -> &ShardComms {
        &self.comms
    }

    pub fn number(&self) -> usize {
        self.number
    }

    pub fn identifier(&self) -> &User {
        &self.identifier
    }

    /// Get currently loaded schema for this shard.
    pub fn schema(&self) -> Schema {
        self.schema.get().cloned().unwrap_or_default()
    }

    /// Re-detect primary/replica roles and re-build
    /// the shard routing logic.
    pub fn redetect_roles(&self) -> bool {
        self.lb.redetect_roles()
    }

    /// Get parameters from first available connection pool.
    pub async fn params(&self, request: &Request) -> Result<&Parameters, Error> {
        self.lb.params(request).await
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
#[derive(Default, Debug, Clone)]
pub struct ShardInner {
    number: usize,
    lb: LoadBalancer,
    comms: Arc<ShardComms>,
    pub_sub: Option<PubSubListener>,
    identifier: Arc<User>,
    schema: Arc<OnceCell<Schema>>,
}

impl ShardInner {
    fn new(shard: ShardConfig) -> Self {
        let ShardConfig {
            number,
            primary,
            replicas,
            lb_strategy,
            rw_split,
            identifier,
            lsn_check_interval,
        } = shard;
        let primary = primary.as_ref().map(Pool::new);
        let lb = LoadBalancer::new(&primary, replicas, lb_strategy, rw_split);
        let comms = Arc::new(ShardComms {
            shutdown: Notify::new(),
            lsn_check_interval,
        });
        let pub_sub = if config().pub_sub_enabled() {
            primary.as_ref().map(PubSubListener::new)
        } else {
            None
        };

        Self {
            number,
            lb,
            comms,
            pub_sub,
            identifier,
            schema: Arc::new(OnceCell::new()),
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use crate::backend::pool::Address;

    use super::*;

    #[tokio::test]
    async fn test_exclude_primary() {
        crate::logger();

        let primary = &Some(PoolConfig {
            address: Address::new_test(),
            ..Default::default()
        });

        let replicas = &[PoolConfig {
            address: Address::new_test(),
            ..Default::default()
        }];

        let shard = Shard::new(ShardConfig {
            number: 0,
            primary,
            replicas,
            lb_strategy: LoadBalancingStrategy::Random,
            rw_split: ReadWriteSplit::ExcludePrimary,
            identifier: Arc::new(User {
                user: "pgdog".into(),
                database: "pgdog".into(),
            }),
            lsn_check_interval: Duration::MAX,
        });
        shard.launch();

        for _ in 0..25 {
            let replica_id = shard.lb.targets[0].pool.id();

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
            ..Default::default()
        });

        let replicas = &[PoolConfig {
            address: Address::new_test(),
            ..Default::default()
        }];

        let shard = Shard::new(ShardConfig {
            number: 0,
            primary,
            replicas,
            lb_strategy: LoadBalancingStrategy::Random,
            rw_split: ReadWriteSplit::IncludePrimary,
            identifier: Arc::new(User {
                user: "pgdog".into(),
                database: "pgdog".into(),
            }),
            lsn_check_interval: Duration::MAX,
        });
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
