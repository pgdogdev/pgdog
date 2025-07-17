//! A shard is a collection of replicas and an optional primary.

use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::spawn;
use tokio::sync::Notify;
use tokio::time::interval;
use tracing::{debug, error};

use crate::config::{config, LoadBalancingStrategy, ReadWriteSplit, Role};
use crate::net::messages::BackendKeyData;

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
        ShardMonitor::run(self);
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

    pub fn get_primary(&self) -> Option<&Pool> {
        self.inner.primary.as_ref()
    }

    pub fn get_replicas(&self) -> &Replicas {
        &self.inner.replicas
    }

    pub fn has_replicas(&self) -> bool {
        !self.inner.replicas.is_empty()
    }

    fn comms(&self) -> &ShardComms {
        &self.inner.comms
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
            comms: ShardComms::default(),
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
        self.comms.shutdown.notify_waiters();
        self.pools().iter().for_each(Pool::shutdown);
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

        let primary = match shard.get_primary() {
            Some(p) => p,
            None => return,
        };

        let min_interval = Duration::from_millis(250);
        let interval_duration = (check_interval / 2).min(min_interval);
        let mut tick = interval(interval_duration);

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
        let Some(primary) = shard.get_primary() else {
            return;
        };

        let Ok(lsn) = primary.wal_flush_lsn().await else {
            error!("primary WAL lsn query failed");
            return;
        };

        for replica in shard.get_replicas().pools() {
            Self::process_single_replica(&replica, lsn, history, max_age).await;
        }
    }

    async fn process_single_replica(
        replica: &Pool,
        primary_lsn: u64,
        history: &Arc<RwLock<RecentLsnHistory>>,
        max_age: Duration,
    ) {
        let replay_lsn = match replica.wal_replay_lsn().await {
            Ok(lsn) => lsn,
            Err(e) => {
                error!("replica {} LSN query failed: {}", replica.id(), e);
                return;
            }
        };

        let h = history.read();
        let maybe_delay = h.estimate_delay(primary_lsn, replay_lsn);

        // TODO(Nic):
        // - The current query logic prevents pools from executing any query once banned.
        // - This includes running their own LSN queries.
        // - For this reason, we cannot ban replicas for lagging just yet
        // - For now, we simply tracing::error!() it for now.
        // - It's sensible to ban replicas from making user queries when it's lagging too much, but...
        //   unexposed PgDog admin queries should be allowed on "banned" replicas.
        // - TLDR; We need a way to distinguish between user and admin queries, and let admin...
        //   queries run on "banned" replicas.

        match maybe_delay {
            Some(est_delay) if est_delay > max_age => {
                error!(
                    "replica {} estimated lag {:?} exceeds {:?}; pausing routing",
                    replica.id(),
                    est_delay,
                    max_age
                );
            }
            Some(est_delay) => {
                debug!("replica {} lag {:?} (estimated)", replica.id(), est_delay);
            }
            None => {}
        }
    }
}

// -------------------------------------------------------------------------------------------------
// ----- ShardMonitoring :: LSN History ------------------------------------------------------------

#[derive(Clone, Copy)]
struct LsnEntry {
    lsn: u64,
    captured_at: Instant,
}

#[derive(Clone)]
struct RecentLsnHistory {
    // Stack-allocated circular buffer
    entries: [LsnEntry; Self::CAPACITY],
    // Points to the next write position
    head: usize,
    // Number of valid entries (0 to CAPACITY)
    len: usize,
}

impl RecentLsnHistory {
    const CAPACITY: usize = 25;

    /// Create a new empty history
    pub fn new() -> Self {
        let empty_entry = LsnEntry {
            lsn: 0,
            captured_at: Instant::now(),
        };

        Self {
            entries: [empty_entry; Self::CAPACITY],
            head: 0,
            len: 0,
        }
    }

    /// Record the latest primary LSN
    pub fn push(&mut self, lsn: u64) {
        let entry = LsnEntry {
            lsn,
            captured_at: Instant::now(),
        };

        self.entries[self.head] = entry;
        self.head = (self.head + 1) % Self::CAPACITY;

        if self.len < Self::CAPACITY {
            self.len += 1;
        }
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

    /// Estimate WAL throughput in bytes per second
    fn wal_throughput(&self) -> Option<f64> {
        if self.len < 2 {
            return None;
        }

        let newest_idx = (self.head + Self::CAPACITY - 1) % Self::CAPACITY;
        let oldest_idx = (self.head + Self::CAPACITY - self.len) % Self::CAPACITY;

        let newest = self.entries[newest_idx];
        let oldest = self.entries[oldest_idx];

        if newest.lsn <= oldest.lsn {
            return None;
        }

        let delta_lsn = newest.lsn - oldest.lsn;
        let delta_time = newest.captured_at.duration_since(oldest.captured_at);

        if delta_time.is_zero() {
            return None;
        }

        Some(delta_lsn as f64 / delta_time.as_secs_f64())
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
