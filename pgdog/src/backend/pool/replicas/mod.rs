//! Replicas pool.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use rand::seq::SliceRandom;
use tokio::{sync::Notify, time::timeout};

use crate::config::{LoadBalancingStrategy, ReadWriteSplit, Role};
use crate::net::messages::BackendKeyData;

use super::{Error, Guard, Pool, PoolConfig, Request};

pub mod ban;
pub mod monitor;
use ban::Ban;
use monitor::*;

#[cfg(test)]
mod test;

/// Replica target containing pool and ban state.
#[derive(Clone, Debug)]
pub struct ReplicaTarget {
    pub pool: Pool,
    pub ban: Ban,
}

impl ReplicaTarget {
    fn new(pool: Pool) -> Self {
        let ban = Ban::new(&pool);
        Self { pool, ban }
    }
}

/// Replicas pools.
#[derive(Clone, Default, Debug)]
pub struct Replicas {
    /// Replica targets (pools with ban state).
    pub(super) replicas: Vec<ReplicaTarget>,
    /// Primary target (pool with ban state).
    pub(super) primary: Option<ReplicaTarget>,
    /// Checkout timeout.
    pub(super) checkout_timeout: Duration,
    /// Round robin atomic counter.
    pub(super) round_robin: Arc<AtomicUsize>,
    /// Chosen load balancing strategy.
    pub(super) lb_strategy: LoadBalancingStrategy,
    /// Shutdown notification.
    pub(super) shutdown: Arc<Notify>,
    /// Read/write split.
    pub(super) rw_split: ReadWriteSplit,
}

impl Replicas {
    /// Create new replicas pools.
    pub fn new(
        primary: &Option<Pool>,
        addrs: &[PoolConfig],
        lb_strategy: LoadBalancingStrategy,
        rw_split: ReadWriteSplit,
    ) -> Replicas {
        let checkout_timeout = addrs
            .iter()
            .map(|c| c.config.checkout_timeout())
            .sum::<Duration>();

        let replicas: Vec<_> = addrs
            .iter()
            .map(|config| ReplicaTarget::new(Pool::new(config)))
            .collect();

        let primary_target = primary
            .as_ref()
            .map(|pool| ReplicaTarget::new(pool.clone()));

        Self {
            primary: primary_target,
            replicas,
            checkout_timeout,
            round_robin: Arc::new(AtomicUsize::new(0)),
            lb_strategy,
            shutdown: Arc::new(Notify::new()),
            rw_split,
        }
    }

    /// Launch replica pools and start the monitor.
    pub fn launch(&self) {
        self.replicas.iter().for_each(|target| target.pool.launch());
        Monitor::new(self);
    }

    /// Get a live connection from the pool.
    pub async fn get(&self, request: &Request) -> Result<Guard, Error> {
        match timeout(self.checkout_timeout, self.get_internal(request)).await {
            Ok(Ok(conn)) => Ok(conn),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(Error::ReplicaCheckoutTimeout),
        }
    }

    /// Move connections from this replica set to another.
    pub fn move_conns_to(&self, destination: &Replicas) {
        assert_eq!(self.replicas.len(), destination.replicas.len());

        for (from, to) in self.replicas.iter().zip(destination.replicas.iter()) {
            from.pool.move_conns_to(&to.pool);
        }
    }

    /// The two replica sets are referring to the same databases.
    pub fn can_move_conns_to(&self, destination: &Replicas) -> bool {
        self.replicas.len() == destination.replicas.len()
            && self
                .replicas
                .iter()
                .zip(destination.replicas.iter())
                .all(|(a, b)| a.pool.can_move_conns_to(&b.pool))
    }

    /// How many replicas we are connected to.
    pub fn len(&self) -> usize {
        self.replicas.len()
    }

    /// There are no replicas.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create new identical replica pool.
    pub fn duplicate(&self) -> Replicas {
        Self {
            replicas: self
                .replicas
                .iter()
                .map(|target| ReplicaTarget::new(target.pool.duplicate()))
                .collect(),
            primary: self
                .primary
                .as_ref()
                .map(|target| ReplicaTarget::new(target.pool.duplicate())),
            checkout_timeout: self.checkout_timeout,
            round_robin: Arc::new(AtomicUsize::new(0)),
            lb_strategy: self.lb_strategy,
            shutdown: Arc::new(Notify::new()),
            rw_split: self.rw_split,
        }
    }

    /// Cancel a query if one is running.
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        for target in &self.replicas {
            target.pool.cancel(id).await?;
        }

        Ok(())
    }

    /// Replica pools handle.
    pub fn pools(&self) -> Vec<&Pool> {
        self.replicas.iter().map(|target| &target.pool).collect()
    }

    /// Collect all connection pools used for read queries.
    pub fn pools_with_roles_and_bans(&self) -> Vec<(Role, Ban, Pool)> {
        let mut result: Vec<_> = self
            .replicas
            .iter()
            .map(|target| (Role::Replica, target.ban.clone(), target.pool.clone()))
            .collect();

        if let Some(ref primary) = self.primary {
            result.push((Role::Primary, primary.ban.clone(), primary.pool.clone()));
        }

        result
    }

    async fn get_internal(&self, request: &Request) -> Result<Guard, Error> {
        let mut candidates: Vec<&ReplicaTarget> = self.replicas.iter().collect();

        let primary_reads = self.rw_split == ReadWriteSplit::IncludePrimary;
        if primary_reads {
            if let Some(ref primary) = self.primary {
                candidates.push(primary);
            }
        }

        use LoadBalancingStrategy::*;

        match self.lb_strategy {
            Random => candidates.shuffle(&mut rand::thread_rng()),
            RoundRobin => {
                let first = self.round_robin.fetch_add(1, Ordering::Relaxed) % candidates.len();
                let mut reshuffled = vec![];
                reshuffled.extend_from_slice(&candidates[first..]);
                reshuffled.extend_from_slice(&candidates[..first]);
                candidates = reshuffled;
            }
            LeastActiveConnections => {
                candidates.sort_by_cached_key(|target| target.pool.lock().idle());
            }
        }

        for target in &candidates {
            if target.ban.banned() {
                continue;
            }
            match target.pool.get(request).await {
                Ok(conn) => return Ok(conn),
                Err(Error::Offline) => continue,
                Err(err) => {
                    // Only ban a candidate pool if there are more than one
                    // and we have alternates.
                    if candidates.len() > 1 {
                        let ban_timeout = target.pool.config().ban_timeout;
                        target.ban.ban(err, ban_timeout);
                    }
                }
            }
        }

        // Unban all targets if all are banned.
        if candidates.iter().all(|target| target.ban.banned()) {
            candidates.iter().for_each(|target| target.ban.unban());
        }

        Err(Error::AllReplicasDown)
    }

    /// Shutdown replica pools.
    ///
    /// N.B. The primary pool is managed by `super::Shard`.
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
        for target in &self.replicas {
            target.pool.shutdown();
        }
    }
}
