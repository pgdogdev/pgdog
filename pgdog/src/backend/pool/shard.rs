//! A shard is a collection of replicas and an optional primary.

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::{interval, sleep};
use tokio::{join, select, spawn, sync::Notify};
use tracing::{debug, error};

use crate::backend::PubSubListener;
use crate::config::{config, LoadBalancingStrategy, ReadWriteSplit, Role};
use crate::net::messages::BackendKeyData;
use crate::net::NotificationResponse;

use super::inner::ReplicaLag;
use super::{Error, Guard, Pool, PoolConfig, Replicas, Request};

// -------------------------------------------------------------------------------------------------
// ----- Public Interface --------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Shard {
    inner: Arc<ShardInner>,
}

impl Shard {
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

    /// Get connection to primary database.
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

            let primary = match self.rw_split {
                IncludePrimary => &self.primary,
                ExcludePrimary => &None,
            };

            self.replicas.get(request, primary).await
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

    pub fn move_conns_to(&self, destination: &Shard) {
        if let Some(ref primary) = self.primary {
            if let Some(ref other) = destination.primary {
                primary.move_conns_to(other);
            }
        }

        self.replicas.move_conns_to(&destination.replicas);
    }

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

    pub fn has_primary(&self) -> bool {
        self.primary.is_some()
    }

    pub fn has_replicas(&self) -> bool {
        !self.replicas.is_empty()
    }

    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        if let Some(ref primary) = self.primary {
            primary.cancel(id).await?;
        }
        self.replicas.cancel(id).await?;

        Ok(())
    }

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

    /// Shutdown every pool.
    pub fn shutdown(&self) {
        self.comms.shutdown.notify_waiters();
        self.pools().iter().for_each(|pool| pool.shutdown());
        if let Some(ref listener) = self.pub_sub {
            listener.shutdown();
        }
    }

    pub fn toggle_pause(&self, pause: bool) {
        for pool in self.pools() {
            if pause {
                pool.pause();
            } else {
                pool.resume();
            }
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

// -------------------------------------------------------------------------------------------------
// ----- Private Implementation --------------------------------------------------------------------

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
        let replicas = Replicas::new(replicas, lb_strategy);
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

// -------------------------------------------------------------------------------------------------
// ----- Comms -------------------------------------------------------------------------------------

#[derive(Debug)]
struct ShardComms {
    pub shutdown: Notify,
}

impl Default for ShardComms {
    fn default() -> Self {
        Self {
            shutdown: Notify::new(),
        }
    }
}

// -------------------------------------------------------------------------------------------------
// ----- Monitoring --------------------------------------------------------------------------------

struct ShardMonitor {}

impl ShardMonitor {
    pub fn run(shard: &Shard) {
        let shard = shard.clone();
        spawn(async move { Self::monitor_replicas(shard).await });
    }
}

impl ShardMonitor {
    async fn monitor_replicas(shard: Shard) {
        let cfg_handle = config();
        let Some(rl_config) = cfg_handle.config.replica_lag.as_ref() else {
            return;
        };

        let mut tick = interval(rl_config.check_interval);

        debug!("replica monitoring running");
        let comms = shard.comms();

        loop {
            select! {
                _ = tick.tick() => {
                    Self::process_replicas(&shard, rl_config.max_age).await;
                }
                _ = comms.shutdown.notified() => break,
            }
        }

        debug!("replica monitoring stopped");
    }

    async fn process_replicas(shard: &Shard, max_age: Duration) {
        let Some(primary) = shard.primary.as_ref() else {
            return;
        };

        primary.set_replica_lag(ReplicaLag::NonApplicable);

        let lsn_metrics = match collect_lsn_metrics(primary, max_age).await {
            Some(m) => m,
            None => {
                error!("failed to collect LSN metrics");
                return;
            }
        };

        for replica in shard.replicas.pools() {
            Self::process_single_replica(
                replica,
                lsn_metrics.max_lsn,
                lsn_metrics.average_bytes_per_sec,
                max_age,
            )
            .await;
        }
    }

    // TODO -> [process_single_replica]
    // - The current query logic prevents pools from executing any query once banned.
    // - This includes running their own LSN queries.
    // - For this reason, we cannot ban replicas for lagging just yet
    // - For now, we simply tracing::error!() it for now.
    // - It's sensible to ban replicas from making user queries when it's lagging too much, but...
    //   unexposed PgDog admin queries should be allowed on "banned" replicas.
    // - TLDR; We need a way to distinguish between user and admin queries, and let admin...
    //   queries run on "banned" replicas.

    async fn process_single_replica(
        replica: &Pool,
        primary_lsn: u64,
        lsn_throughput: f64,
        _max_age: Duration, // used to make banning decisions when it's supported later
    ) {
        if replica.banned() {
            replica.set_replica_lag(ReplicaLag::Unknown);
            return;
        };

        let replay_lsn = match replica.wal_replay_lsn().await {
            Ok(lsn) => lsn,
            Err(e) => {
                error!("replica {} LSN query failed: {}", replica.id(), e);
                return;
            }
        };

        let bytes_behind = primary_lsn.saturating_sub(replay_lsn);

        let mut lag = ReplicaLag::Bytes(bytes_behind);
        if lsn_throughput > 0.0 {
            let duration = Duration::from_secs_f64(bytes_behind as f64 / lsn_throughput);
            lag = ReplicaLag::Duration(duration);
        }
        if bytes_behind == 0 {
            lag = ReplicaLag::Duration(Duration::ZERO);
        }

        replica.set_replica_lag(lag);
    }
}

// -------------------------------------------------------------------------------------------------
// ----- Utils :: Primary LSN Metrics --------------------------------------------------------------

struct LsnMetrics {
    pub average_bytes_per_sec: f64,
    pub max_lsn: u64,
}

/// Sample WAL LSN at 0, half, and full window; compute avg rate and max LSN.
async fn collect_lsn_metrics(primary: &Pool, window: Duration) -> Option<LsnMetrics> {
    if window.is_zero() {
        return None;
    }

    let half_window = window / 2;
    let half_window_in_seconds = half_window.as_secs_f64();

    // fire three futures at once
    let f0 = primary.wal_flush_lsn();
    let f1 = async {
        sleep(half_window).await;
        primary.wal_flush_lsn().await
    };
    let f2 = async {
        sleep(window).await;
        primary.wal_flush_lsn().await
    };

    // collect results concurrently
    let (r0, r1, r2) = join!(f0, f1, f2);

    let lsn_initial = r0.ok()?;
    let lsn_half = r1.ok()?;
    let lsn_full = r2.ok()?;

    let rate1 = (lsn_half.saturating_sub(lsn_initial)) as f64 / half_window_in_seconds;
    let rate2 = (lsn_full.saturating_sub(lsn_half)) as f64 / half_window_in_seconds;
    let average_rate = (rate1 + rate2) / 2.0;

    let max_lsn = lsn_initial.max(lsn_half).max(lsn_full);

    let metrics = LsnMetrics {
        average_bytes_per_sec: average_rate,
        max_lsn,
    };

    Some(metrics)
}

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

        let shard = Shard::new(
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

// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------
