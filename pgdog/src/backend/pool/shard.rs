//! A shard is a collection of replicas and an optional primary.

use parking_lot::RwLock;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::spawn;
use tokio::sync::Notify;
use tokio::time::interval;
use tracing::{debug, error};

use crate::config::{config, LoadBalancingStrategy, ReadWriteSplit, Role};
use crate::net::messages::BackendKeyData;

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

    pub async fn primary(&self, request: &Request) -> Result<Guard, Error> {
        self.primary
            .as_ref()
            .ok_or(Error::NoPrimary)?
            .get_forced(request)
            .await
    }

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

    /// Clone pools but keep them independent.
    pub fn duplicate(&self) -> Self {
        Self {
            inner: Arc::new(ShardInner {
                primary: self
                    .inner
                    .primary
                    .as_ref()
                    .map(|primary| primary.duplicate()),
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

    pub fn shutdown(&self) {
        self.comms.shutdown.notify_waiters();
        self.pools().iter().for_each(|pool| pool.shutdown());
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

        Self {
            primary,
            replicas,
            rw_split,
            comms,
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
        let history: Arc<RwLock<RecentLsnHistory>> = Arc::default();

        {
            let shard = shard.clone();
            let hist = history.clone();
            spawn(async move { Self::track_primary_lsn(shard, hist).await });
        }

        {
            let shard = shard.clone();
            let hist = history.clone();
            spawn(async move { Self::monitor_replicas(shard, hist).await });
        }
    }
}

impl ShardMonitor {
    async fn track_primary_lsn(shard: Shard, hist: Arc<RwLock<RecentLsnHistory>>) {
        let check_interval = match config()
            .config
            .replica_lag
            .as_ref()
            .map(|rl| rl.check_interval)
        {
            Some(v) => v,
            None => return,
        };

        let primary = match shard.primary.as_ref() {
            Some(p) => p,
            None => return,
        };

        let min_interval = Duration::from_millis(250);
        let interval_duration = (check_interval / 2).min(min_interval);
        let mut tick = interval(interval_duration);

        primary.set_replica_lag(ReplicaLag::NonApplicable);

        debug!("primary LSN tracking running");
        let comms = shard.comms();

        loop {
            select! {
                _ = tick.tick() => {
                    if let Ok(lsn) = primary.wal_flush_lsn().await {
                        if let Some(mut h) = hist.try_write() {
                            h.push(lsn);
                        }
                    } else {
                        error!("primary WAL lsn query failed");
                    }
                }

                _ = comms.shutdown.notified() => break,
            }
        }
    }
}

impl ShardMonitor {
    async fn monitor_replicas(shard: Shard, hist: Arc<RwLock<RecentLsnHistory>>) {
        let cfg_handle = config();
        let Some(replica_lag) = cfg_handle.config.replica_lag.as_ref() else {
            return;
        };

        let mut tick = interval(replica_lag.check_interval);

        debug!("replica monitoring running");
        let comms = shard.comms();

        loop {
            select! {
                _ = tick.tick() => {
                    Self::process_replicas(&shard, &hist, replica_lag.max_age).await;
                }
                _ = comms.shutdown.notified() => break,
            }
        }

        debug!("replica monitoring stopped");
    }

    async fn process_replicas(
        shard: &Shard,
        history: &Arc<RwLock<RecentLsnHistory>>,
        max_age: Duration,
    ) {
        let Some(primary) = shard.primary.as_ref() else {
            return;
        };

        let Ok(lsn) = primary.wal_flush_lsn().await else {
            error!("primary WAL lsn query failed");
            return;
        };

        for replica in shard.replicas.pools() {
            Self::process_single_replica(&replica, lsn, history, max_age).await;
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
        history: &Arc<RwLock<RecentLsnHistory>>,
        max_age: Duration,
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

        let h = history.read();
        let maybe_delay = h.estimate_delay(primary_lsn, replay_lsn);

        match maybe_delay {
            Some(delay) if delay > max_age => {
                error!(
                    "replica {} estimated lag {:?} exceeds {:?}; should ban replica",
                    replica.id(),
                    delay,
                    max_age
                );
            }
            Some(delay) => {
                debug!("replica {} lag {:?} (estimated)", replica.id(), delay);
            }
            None => {}
        }

        let replica_lag = h.get_replica_lag(primary_lsn, replay_lsn);
        replica.set_replica_lag(replica_lag);
    }
}

// -------------------------------------------------------------------------------------------------
// ----- Monitoring :: LSN History -----------------------------------------------------------------

#[derive(Clone, Copy)]
struct LsnEntry {
    lsn: u64,
    captured_at: Instant,
}

pub struct RecentLsnHistory {
    entries: VecDeque<LsnEntry>,
}

impl RecentLsnHistory {
    const CAPACITY: usize = 25;

    /// Create a new empty history
    pub fn new() -> Self {
        Self {
            entries: VecDeque::with_capacity(Self::CAPACITY),
        }
    }

    /// Record the latest primary LSN
    pub fn push(&mut self, lsn: u64) {
        if self.entries.len() == Self::CAPACITY {
            self.entries.pop_front();
        }

        self.entries.push_back(LsnEntry {
            lsn,
            captured_at: Instant::now(),
        });
    }

    pub fn get_replica_lag(&self, primary_lsn: u64, replay_lsn: u64) -> ReplicaLag {
        let bytes_behind = primary_lsn.saturating_sub(replay_lsn);
        if bytes_behind == 0 {
            return ReplicaLag::Duration(Duration::ZERO);
        }

        if let Some(delay) = self.estimate_delay(primary_lsn, replay_lsn) {
            return ReplicaLag::Duration(delay);
        };

        ReplicaLag::Bytes(bytes_behind)
    }

    pub fn estimate_delay(&self, primary_lsn: u64, replay_lsn: u64) -> Option<Duration> {
        let bytes_behind = primary_lsn.saturating_sub(replay_lsn);
        if bytes_behind == 0 {
            return Some(Duration::ZERO);
        }

        let bytes_per_second = self.wal_throughput()?;
        if bytes_per_second <= 0.0 {
            return None;
        }

        let seconds_behind = bytes_behind as f64 / bytes_per_second;

        Some(Duration::from_secs_f64(seconds_behind))
    }

    fn wal_throughput(&self) -> Option<f64> {
        if self.entries.len() < 2 {
            return None;
        }

        let oldest = self.entries.front().unwrap();
        let newest = self.entries.back().unwrap();

        let elapsed = newest.captured_at.duration_since(oldest.captured_at);
        let lsn_delta = newest.lsn.checked_sub(oldest.lsn)?;

        if elapsed.is_zero() {
            return None;
        }

        Some(lsn_delta as f64 / elapsed.as_secs_f64())
    }
}

impl Default for RecentLsnHistory {
    fn default() -> Self {
        Self::new()
    }
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
