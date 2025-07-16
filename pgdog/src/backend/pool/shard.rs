//! A shard is a collection of replicas and an optional primary.

use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::spawn;
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

struct ShardMonitor {}

impl ShardMonitor {
    pub fn run(shard: &Shard) {
        let history: Arc<RwLock<RecentLsnHistory>> = Arc::default();

        println!("WHAT THE FUCK...");

        // ----- primary loop -----------------------------------------
        {
            let shard = shard.clone();
            let hist = history.clone();
            spawn(async move { Self::monitor_primary(shard, hist).await });
        }

        // -----replica loop -----------------------------------------
        {
            let shard = shard.clone();
            let hist = history.clone();
            spawn(async move { Self::monitor_replicas(shard, hist).await });
        }
    }

    async fn monitor_primary(shard: Shard, hist: Arc<RwLock<RecentLsnHistory>>) {
        let max_age = match config().config.replica_lag.as_ref().map(|rl| rl.max_age) {
            Some(v) => v,
            None => return,
        };

        let primary = match shard.get_primary() {
            Some(p) => p,
            None => return,
        };

        let mut tick = interval(max_age);

        loop {
            if let Ok(lsn) = primary.wal_flush_lsn().await {
                if let Some(mut h) = hist.try_write() {
                    h.push(lsn);
                }
            } else {
                error!("primary lsn query failed");
            }

            tick.tick().await;
        }
    }

    async fn monitor_replicas(shard: Shard, hist: Arc<RwLock<RecentLsnHistory>>) {
        let cfg_handle = config();
        let Some(replica_lag) = cfg_handle.config.replica_lag.as_ref() else {
            return;
        };

        if !shard.has_replicas() {
            return;
        }

        let mut tick = interval(replica_lag.check_interval);

        loop {
            for replica in shard.get_replicas().pools() {
                match replica.wal_replay_lsn().await {
                    Ok(replay_lsn) => {
                        let delay_opt = hist.read().delay_for(replay_lsn);
                        match delay_opt {
                            Some(delay) if delay > replica_lag.max_age => {
                                println!("");
                                println!("");
                                println!("");
                                println!("");
                                println!("");
                                println!("");
                                error!(
                                    "replica {} lag {:?} exceeds {:?}; pausing routing",
                                    replica.id(),
                                    delay,
                                    replica_lag.max_age
                                );
                            }
                            Some(delay) => {
                                println!("");
                                println!("");
                                println!("");
                                println!("");
                                println!("");
                                println!("");
                                debug!("replica {} lag {:?}", replica.id(), delay);
                            }
                            None => {
                                println!("");
                                println!("");
                                println!("");
                                println!("");
                                println!("");
                                error!(
                                    "replica {} LSN {} older than history window",
                                    replica.id(),
                                    replay_lsn
                                );
                            }
                        }
                    }
                    Err(e) => {
                        println!("");
                        println!("");
                        println!("");
                        println!("");
                        println!("");
                        error!("replica {} LSN query failed: {}", replica.id(), e);
                    }
                }
            }
            tick.tick().await;
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

#[derive(Default)]
struct RecentLsnHistory {
    entries: Vec<LsnEntry>,
}

impl RecentLsnHistory {
    const CAPACITY: usize = 20;

    /// record the latest primary LSN
    fn push(&mut self, lsn: u64) {
        let entry = LsnEntry {
            lsn,
            captured_at: Instant::now(),
        };

        self.entries.push(entry);
        if self.entries.len() > Self::CAPACITY {
            self.entries.remove(0);
        }
    }

    /// Estimate replica delay.
    fn delay_for(&self, replay_lsn: u64) -> Option<Duration> {
        if self.entries.len() < Self::CAPACITY {
            return Some(Duration::ZERO);
        }

        let now = Instant::now();
        for entry in self.entries.iter().rev() {
            if entry.lsn <= replay_lsn {
                return Some(now.duration_since(entry.captured_at));
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn bench_push_and_delay_for() {
        let mut hist = RecentLsnHistory::default();
        let ops: u32 = 100_000;

        // warm up buffer
        for i in 0..(RecentLsnHistory::CAPACITY as u32) {
            hist.push(i as u64);
        }

        // measure push()
        let start_push = Instant::now();
        for i in 0..ops {
            hist.push(i as u64);
        }
        let push_duration = start_push.elapsed();
        println!(
            "push: {} ops in {:?} (avg {:?})",
            ops,
            push_duration,
            push_duration / ops
        );

        // measure delay_for()
        let sample_lsn = (ops / 2) as u64;
        let start_delay = Instant::now();
        for _ in 0..ops {
            let _ = hist.delay_for(sample_lsn);
        }
        let delay_duration = start_delay.elapsed();
        println!(
            "delay_for: {} ops in {:?} (avg {:?})",
            ops,
            delay_duration,
            delay_duration / ops
        );
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
