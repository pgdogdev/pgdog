//! Replicas pool.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use rand::seq::SliceRandom;
use tokio::{
    sync::Notify,
    time::{timeout, Instant},
};

use crate::net::messages::BackendKeyData;
use crate::{
    backend::pool::shard::role_detector::DetectedRoles,
    config::{LoadBalancingStrategy, ReadWriteSplit, Role},
};

use super::{Error, Guard, Pool, PoolConfig, Request};

pub mod ban;
pub mod detected_role;
pub mod monitor;
pub mod target_health;

use ban::Ban;
pub use detected_role::*;
use monitor::*;
pub use target_health::*;

#[cfg(test)]
mod test;

/// Read query load balancer target.
#[derive(Clone, Debug)]
pub struct ReadTarget {
    pub pool: Pool,
    pub ban: Ban,
    pub role: Role,
    pub health: TargetHealth,
}

impl ReadTarget {
    pub(super) fn new(pool: Pool, role: Role) -> Self {
        let ban = Ban::new(&pool);
        Self {
            ban,
            role,
            health: pool.inner().health.clone(),
            pool,
        }
    }
}

/// Replicas pools.
#[derive(Clone, Default, Debug)]
pub struct Replicas {
    /// Replica targets (pools with ban state).
    pub(super) replicas: Vec<ReadTarget>,
    /// Primary target (pool with ban state).
    pub(super) primary: Option<ReadTarget>,
    /// Checkout timeout.
    pub(super) checkout_timeout: Duration,
    /// Round robin atomic counter.
    pub(super) round_robin: Arc<AtomicUsize>,
    /// Chosen load balancing strategy.
    pub(super) lb_strategy: LoadBalancingStrategy,
    /// Maintenance. notification.
    pub(super) maintenance: Arc<Notify>,
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
            .map(|c| c.config.checkout_timeout)
            .sum::<Duration>();

        let replicas: Vec<_> = addrs
            .iter()
            .map(|config| ReadTarget::new(Pool::new(config), Role::Replica))
            .collect();

        let primary_target = primary
            .as_ref()
            .map(|pool| ReadTarget::new(pool.clone(), Role::Primary));

        Self {
            primary: primary_target,
            replicas,
            checkout_timeout,
            round_robin: Arc::new(AtomicUsize::new(0)),
            lb_strategy,
            maintenance: Arc::new(Notify::new()),
            rw_split,
        }
    }

    /// Get current database roles.
    pub fn current_roles(&self) -> DetectedRoles {
        let mut roles = self
            .replicas
            .iter()
            .map(|replica| {
                let role = DetectedRole::from_read_target(replica);
                (role.database_number, role)
            })
            .collect::<HashMap<_, _>>();

        if let Some(ref primary) = self.primary {
            let role = DetectedRole::from_read_target(primary);
            roles.insert(role.database_number, role);
        }

        roles.into()
    }

    /// Detect database roles from pg_is_in_recovery() and
    /// return new primary (if any), and replicas.
    pub fn redetect_roles(&self) -> Option<DetectedRoles> {
        let mut targets = self
            .replicas
            .clone()
            .into_iter()
            .map(|target| (target.pool.lsn_stats(), target))
            .collect::<Vec<_>>();

        // Only detect roles if the LSN detector is running.
        if !targets.iter().all(|target| target.0.valid()) {
            return None;
        }

        if let Some(primary) = self.primary.clone() {
            targets.push((primary.pool.lsn_stats(), primary));
        }

        // Pick primary by latest data. The one with the most
        // up-to-date lsn number and pg_is_in_recovery() = false
        // is the new primary.
        //
        // The old primary is still part of the config and will be demoted
        // to replica. If it's down, it will be banned from serving traffic.
        //
        let now = Instant::now();
        targets.sort_by_cached_key(|target| target.0.lsn_age(now));

        let primary = targets
            .iter()
            .find(|target| target.0.valid() && !target.0.replica);
        let replicas = targets
            .iter()
            .filter(|target| target.0.replica)
            .collect::<Vec<_>>();

        let mut numbers: HashMap<_, _> = replicas
            .iter()
            .map(|target| {
                let database_number = target.1.pool.addr().database_number;
                (
                    database_number,
                    DetectedRole {
                        role: Role::Replica,
                        as_of: target.0.fetched,
                        database_number,
                    },
                )
            })
            .collect();
        if let Some(primary) = primary {
            let database_number = primary.1.pool.addr().database_number;

            numbers.insert(
                database_number,
                DetectedRole {
                    role: Role::Primary,
                    as_of: primary.0.fetched,
                    database_number,
                },
            );
        }

        Some(numbers.into())
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
        use LoadBalancingStrategy::*;
        use ReadWriteSplit::*;

        let mut candidates: Vec<&ReadTarget> = self.replicas.iter().collect();

        let primary_reads = match self.rw_split {
            IncludePrimary => true,
            IncludePrimaryIfReplicaBanned => candidates.iter().any(|target| target.ban.banned()),
            ExcludePrimary => false,
        };

        if primary_reads {
            if let Some(ref primary) = self.primary {
                candidates.push(primary);
            }
        }

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

        // Only ban a candidate pool if there are more than one
        // and we have alternates.
        let bannable = candidates.len() > 1;

        for target in &candidates {
            if target.ban.banned() {
                continue;
            }
            match target.pool.get(request).await {
                Ok(conn) => return Ok(conn),
                Err(Error::Offline) => {
                    continue;
                }
                Err(err) => {
                    if bannable {
                        target.ban.ban(err, target.pool.config().ban_timeout);
                    }
                }
            }
        }

        candidates.iter().for_each(|target| target.ban.unban(true));

        Err(Error::AllReplicasDown)
    }

    /// Shutdown replica pools.
    ///
    /// N.B. The primary pool is managed by `super::Shard`.
    pub fn shutdown(&self) {
        for target in &self.replicas {
            target.pool.shutdown();
        }

        self.maintenance.notify_waiters();
    }
}
