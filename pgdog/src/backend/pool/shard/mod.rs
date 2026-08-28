//! A shard is a collection of replicas and an optional primary.

use arc_swap::ArcSwap;
use futures::try_join;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::SetOnce;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::backend::PubSubListener;
use crate::backend::Schema;
use crate::backend::databases::User;
use crate::backend::pool::lb::ban::Ban;
use crate::backend::pub_sub::listener::Listener;
use crate::backend::schema::SchemaCache;
use crate::config::{LoadBalancingStrategy, ReadWriteSplit, Role};
use crate::net::Parameters;
use crate::net::messages::FrontendPid;

use super::{Error, Guard, LoadBalancer, Pool, PoolConfig, Request};

pub(crate) mod monitor;
mod oids;
pub(crate) mod role_detector;

use monitor::*;
pub(crate) use oids::{CanonicalOids, Oids};
use role_detector::*;

#[cfg_attr(test, derive(Default))]
pub(super) struct ShardConfig<'a> {
    /// Shard number.
    pub(super) number: usize,
    /// Shard primary database, if any.
    pub(super) primary: Option<&'a PoolConfig>,
    /// Shard replica databases.
    pub(super) replicas: &'a [PoolConfig],
    /// Load balancing strategy for replicas.
    pub(super) lb_strategy: LoadBalancingStrategy,
    /// Primary/replica read/write split strategy.
    pub(super) rw_split: ReadWriteSplit,
    /// Cluster identifier (user/database).
    pub(super) identifier: Arc<User>,
    /// LSN check interval
    pub(super) lsn_check_interval: Duration,
    /// Pub/sub enabled
    pub(super) pub_sub_enabled: bool,
    /// Global schema cache.
    pub(super) schema_cache: SchemaCache,
}

/// Connection pools for a single database shard.
///
/// Includes a primary and replicas.
#[derive(Clone, Debug)]
pub(crate) struct Shard {
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
    pub(crate) async fn primary(&self, request: &Request) -> Result<Guard, Error> {
        self.lb.get_primary(request).await
    }

    /// Get connection to one of the replica databases, using the configured
    /// load balancing algorithm.
    pub(crate) async fn replica(&self, request: &Request) -> Result<Guard, Error> {
        self.lb.get(request).await
    }

    /// Get connection to primary if configured, otherwise replica.
    pub(crate) async fn primary_or_replica(&self, request: &Request) -> Result<Guard, Error> {
        match self.primary(request).await {
            Ok(primary) => Ok(primary),
            _ => self.replica(request).await,
        }
    }

    /// Move connections from this shard to another shard, preserving them.
    ///
    /// This is done during configuration reloading, if no significant changes are made to
    /// the configuration.
    pub(crate) fn move_conns_to(&self, destination: &Shard) -> Result<(), Error> {
        self.lb.move_conns_to(&destination.lb)?;

        Ok(())
    }

    /// Checks if the connection pools from this shard are compatible
    /// with the other shard. If yes, they can be moved without closing them.
    pub(crate) fn can_move_conns_to(&self, other: &Shard) -> bool {
        self.lb.can_move_conns_to(&other.lb)
    }

    /// Listen for notifications on channel.
    pub(crate) async fn listen(&self, channel: &str) -> Result<Listener, Error> {
        match self.pub_sub.load_full().deref() {
            Some(listener) => listener.listen(channel).await,
            _ => Err(Error::PubSubDisabled),
        }
    }

    /// Notify channel with optional payload (payload can be empty string).
    pub(crate) async fn notify(&self, channel: &str, payload: &str) -> Result<(), Error> {
        match self.pub_sub.load_full().deref() {
            Some(listener) => listener.notify(channel, payload).await,
            _ => Err(Error::PubSubDisabled),
        }
    }

    /// Load schema from the shard's primary database.
    ///
    /// Uses the global schema cache, so most requests will not actually touch
    /// the database.
    ///
    pub(crate) async fn load_schema(&self) -> Result<bool, crate::backend::Error> {
        if self.schema.initialized() {
            return Ok(false);
        }

        // This is syncrhonized by database/shard number, so this prevents
        // a thundering herd with 100s of users, for example, all fetching
        // the same schema.
        let (_, schema) = try_join!(self.oids.load(self), self.schema_cache.get(self))?;
        self.schema.set(schema).expect("schema was not initialized");

        Ok(true)
    }

    /// Fetch schema from the shard. This does not use the
    /// cache and returns the freshed schema available.
    ///
    pub(crate) async fn fetch_schema(&self) -> Result<Schema, crate::backend::Error> {
        let mut server = self.primary_or_replica(&Request::default()).await?;
        let schema = Schema::load(&mut server).await?;

        info!(
            "loaded schema for {} tables on shard {} [{}]",
            schema.tables().len(),
            self.number(),
            server.addr()
        );

        Ok(schema)
    }

    /// Set the schema to its default value.
    /// We don't need it for this shard.
    pub(super) fn schema_not_needed(&self) {
        let _ = self.schema.set(Schema::default());
        self.skip_loading_oids()
    }

    /// Skip loading this shard's type information
    pub(super) fn skip_loading_oids(&self) {
        self.oids.skip_load();
    }

    /// Wait for the shard to load the schema.
    /// If the schema is loaded already, this returns immediately.
    pub(super) async fn wait_schema_loaded(&self) {
        self.schema.wait().await;
        self.oids.wait().await;
    }

    /// Check that the shard LB targets are all launched.
    pub(crate) fn online(&self) -> bool {
        self.lb.online()
    }

    /// Bring every pool online.
    pub(crate) fn launch(&self) {
        self.lb.launch();
        ShardMonitor::run(self);
        self.init_pub_sub();
    }

    /// Returns true if the shard has a primary database.
    pub(crate) fn has_primary(&self) -> bool {
        self.lb.primary().is_some() || self.lb.role_detection_enabled()
    }

    /// Returns true if the shard has any replica databases.
    pub(crate) fn has_replicas(&self) -> bool {
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
    pub(crate) async fn cancel(&self, id: FrontendPid) -> Result<(), super::super::Error> {
        self.lb.cancel(id).await?;

        Ok(())
    }

    /// Get all connection pools.
    pub(crate) fn pools(&self) -> Vec<Pool> {
        self.pools_with_roles()
            .into_iter()
            .map(|(_, pool)| pool)
            .collect()
    }

    /// Get a reference to all pools managed by this shard.
    pub(crate) fn pool_iter(&self) -> impl Iterator<Item = &Pool> {
        self.lb.targets.iter().map(|target| &target.pool)
    }

    /// Get all connection pools along with their roles (i.e., primary or replica).
    pub(crate) fn pools_with_roles(&self) -> Vec<(Role, Pool)> {
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
    pub(crate) fn pools_with_roles_and_bans(&self) -> Vec<(Role, Ban, Pool)> {
        self.lb.pools_with_roles_and_bans()
    }

    /// Shutdown every pool and maintenance task in this shard.
    pub(crate) fn shutdown(&self) {
        self.comms.shutdown.cancel();
        self.shutdown_pub_sub();
        self.lb.shutdown();
    }

    fn comms(&self) -> &ShardComms {
        &self.comms
    }

    pub(crate) fn number(&self) -> usize {
        self.number
    }

    pub(crate) fn identifier(&self) -> &User {
        &self.identifier
    }

    /// Get currently loaded schema for this shard, or an empty schema if
    /// the schema was not yet loaded.
    pub(crate) fn schema(&self) -> Schema {
        self.schema.get().cloned().unwrap_or_default()
    }

    /// Re-detect primary/replica roles and re-build
    /// the shard routing logic.
    pub(crate) fn redetect_roles(&self) -> bool {
        self.lb.redetect_roles()
    }

    /// Get parameters from first available connection pool.
    pub(crate) async fn params(&self, request: &Request) -> Result<&Parameters, Error> {
        self.lb.params(request).await
    }

    /// (Re)initialize the pub/sub listener.
    pub(crate) fn init_pub_sub(&self) {
        if self.inner.pub_sub_enabled {
            // Create new listener.
            // This is useful if we promoted a primary
            // from a replica.
            let primary = self.lb.primary().cloned();
            let pub_sub = primary
                .as_ref()
                .map(|primary| PubSubListener::new(primary, self.identifier(), self.number()));

            // Launch the new listener first!
            if let Some(ref pub_sub) = pub_sub {
                pub_sub.launch();
            }

            // Shutdown the old listener.
            if let Some(pub_sub) = self.inner.pub_sub.swap(Arc::new(pub_sub)).deref() {
                pub_sub.shutdown();
            }
        }
    }

    /// Shutdown pub/sub listener.
    fn shutdown_pub_sub(&self) {
        if let Some(pub_sub) = self.inner.pub_sub.swap(Arc::new(None)).deref() {
            pub_sub.shutdown();
        }
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
#[cfg_attr(test, derive(Default))]
#[derive(Debug)]
pub(crate) struct ShardInner {
    number: usize,
    lb: LoadBalancer,
    comms: Arc<ShardComms>,
    pub_sub: Arc<ArcSwap<Option<PubSubListener>>>,
    identifier: Arc<User>,
    schema: SetOnce<Schema>,
    pub_sub_enabled: bool,
    schema_cache: SchemaCache,
    oids: Arc<Oids>,
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
            pub_sub_enabled,
            schema_cache,
        } = shard;
        let oids = schema_cache.oids(&identifier.database, number);
        let primary = primary.map(|config| Pool::with_oid_mapping(config, Arc::clone(&oids)));
        let lb = LoadBalancer::new(&primary, replicas, lb_strategy, rw_split, Arc::clone(&oids));
        let comms = Arc::new(ShardComms {
            shutdown: CancellationToken::new(),
            lsn_check_interval,
        });

        Self {
            number,
            lb,
            comms,
            pub_sub: Arc::new(ArcSwap::new(Arc::new(None))),
            identifier,
            schema: SetOnce::new(),
            pub_sub_enabled,
            schema_cache,
            oids,
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

        let primary = Some(&PoolConfig {
            address: Address::new_test(),
            ..Default::default()
        });

        let replicas = &[PoolConfig {
            address: Address {
                configured_role: Role::Replica,
                ..Address::new_test()
            },
            ..Default::default()
        }];

        let shard = Shard::new(ShardConfig {
            primary,
            replicas,
            rw_split: ReadWriteSplit::ExcludePrimary,
            identifier: Arc::new(User {
                user: "pgdog".into(),
                database: "pgdog".into(),
            }),
            lsn_check_interval: Duration::MAX,
            ..Default::default()
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

        let primary = Some(&PoolConfig {
            address: Address::new_test(),
            ..Default::default()
        });

        let replicas = &[PoolConfig {
            address: Address::new_test(),
            ..Default::default()
        }];

        let shard = Shard::new(ShardConfig {
            primary,
            replicas,
            rw_split: ReadWriteSplit::IncludePrimary,
            identifier: Arc::new(User {
                user: "pgdog".into(),
                database: "pgdog".into(),
            }),
            lsn_check_interval: Duration::MAX,
            ..Default::default()
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

    #[test]
    fn test_auto_mode_is_read_ready_while_primary_election_is_pending() {
        let replicas = &[PoolConfig {
            address: Address {
                configured_role: Role::Auto,
                ..Address::new_test()
            },
            config: super::super::Config {
                role_detection: true,
                ..Default::default()
            },
        }];

        let shard = Shard::new(ShardConfig {
            replicas,
            identifier: Arc::new(User {
                user: "pgdog".into(),
                database: "pgdog".into(),
            }),
            ..Default::default()
        });

        assert!(shard.has_primary());
        assert!(shard.has_replicas());
        assert_eq!(shard.lb.targets[0].role(), Role::Replica);
    }
}
