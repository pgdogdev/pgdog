//! Load balanced connection pool.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use rand::seq::SliceRandom;
use tokio::{sync::Notify, time::timeout};
use tracing::warn;

use crate::config::{LoadBalancingStrategy, ReadWriteSplit, Role};
use crate::net::messages::BackendKeyData;

use super::{Error, Guard, Pool, PoolConfig, Request};

pub mod ban;
pub mod monitor;
pub mod target_health;

use ban::Ban;
use monitor::*;
pub use target_health::*;

#[cfg(test)]
mod test;

/// Read query load balancer target.
#[derive(Clone, Debug)]
pub struct Target {
    pub pool: Pool,
    pub ban: Ban,
    replica: Arc<AtomicBool>,
    pub health: TargetHealth,
}

impl Target {
    pub(super) fn new(pool: Pool, role: Role) -> Self {
        let ban = Ban::new(&pool);
        Self {
            ban,
            replica: Arc::new(AtomicBool::new(role == Role::Replica)),
            health: pool.inner().health.clone(),
            pool,
        }
    }

    /// Get role.
    pub(super) fn role(&self) -> Role {
        if self.replica.load(Ordering::Relaxed) {
            Role::Replica
        } else {
            Role::Primary
        }
    }

    /// Set role.
    pub(super) fn set_role(&self, role: Role) -> bool {
        let value = role == Role::Replica;
        let old = self.replica.swap(value, Ordering::Relaxed);
        value != old
    }
}

/// Load balancer.
#[derive(Clone, Default, Debug)]
pub struct LoadBalancer {
    /// Read/write targets.
    pub(super) targets: Vec<Target>,
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

impl LoadBalancer {
    /// Create new replicas pools.
    pub fn new(
        primary: &Option<Pool>,
        addrs: &[PoolConfig],
        lb_strategy: LoadBalancingStrategy,
        rw_split: ReadWriteSplit,
    ) -> LoadBalancer {
        let checkout_timeout = primary
            .as_ref()
            .map(|primary| primary.config().checkout_timeout)
            .unwrap_or(Duration::ZERO)
            + addrs
                .iter()
                .map(|c| c.config.checkout_timeout)
                .sum::<Duration>();

        let mut targets: Vec<_> = addrs
            .iter()
            .map(|config| Target::new(Pool::new(config), Role::Replica))
            .collect();

        let primary_target = primary
            .as_ref()
            .map(|pool| Target::new(pool.clone(), Role::Primary));

        if let Some(primary) = primary_target {
            targets.push(primary);
        }

        Self {
            targets,
            checkout_timeout,
            round_robin: Arc::new(AtomicUsize::new(0)),
            lb_strategy,
            maintenance: Arc::new(Notify::new()),
            rw_split,
        }
    }

    /// Get the primary pool, if configured.
    pub fn primary(&self) -> Option<&Pool> {
        self.primary_target().map(|target| &target.pool)
    }

    /// Get the primary read target containing the pool, ban state, and health.
    ///
    /// Unlike [`primary()`], this returns the full target struct which allows
    /// access to ban and health state for monitoring and testing purposes.
    pub fn primary_target(&self) -> Option<&Target> {
        self.targets
            .iter()
            .rev() // If there is a primary, it's likely to be last.
            .find(|target| target.role() == Role::Primary)
    }

    /// Detect database roles from pg_is_in_recovery() and
    /// return new primary (if any), and replicas.
    pub fn redetect_roles(&self) -> bool {
        let mut promoted = false;

        let mut targets = self
            .targets
            .clone()
            .into_iter()
            .map(|target| (target.pool.lsn_stats(), target))
            .collect::<Vec<_>>();

        // Pick primary by latest data. The one with the most
        // up-to-date lsn number and pg_is_in_recovery() = false
        // is the new primary.
        //
        // The old primary is still part of the config and will be demoted
        // to replica. If it's down, it will be banned from serving traffic.
        //
        let now = SystemTime::now();
        targets.sort_by_cached_key(|target| target.0.lsn_age(now));

        let primary = targets
            .iter()
            .position(|target| !target.0.replica && target.0.valid());

        if let Some(primary) = primary {
            promoted = targets[primary].1.set_role(Role::Primary);

            if promoted {
                warn!("new primary chosen: {}", targets[primary].1.pool.addr());

                // Demote everyone else to replicas.
                targets
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != primary)
                    .for_each(|(_, target)| {
                        target.1.set_role(Role::Replica);
                    });
            }
        }

        promoted
    }

    /// Launch replica pools and start the monitor.
    pub fn launch(&self) {
        self.targets.iter().for_each(|target| target.pool.launch());
        Monitor::spawn(self);
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
    pub fn move_conns_to(&self, destination: &LoadBalancer) -> Result<(), Error> {
        assert_eq!(self.targets.len(), destination.targets.len());

        for (from, to) in self.targets.iter().zip(destination.targets.iter()) {
            from.pool.move_conns_to(&to.pool)?;
        }

        Ok(())
    }

    /// The two replica sets are referring to the same databases.
    pub fn can_move_conns_to(&self, destination: &LoadBalancer) -> bool {
        self.targets.len() == destination.targets.len()
            && self
                .targets
                .iter()
                .zip(destination.targets.iter())
                .all(|(a, b)| a.pool.can_move_conns_to(&b.pool))
    }

    /// There are no replicas.
    pub fn has_replicas(&self) -> bool {
        self.targets
            .iter()
            .any(|target| target.role() == Role::Replica)
    }

    /// Cancel a query if one is running.
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        for target in &self.targets {
            target.pool.cancel(id).await?;
        }

        Ok(())
    }

    /// Replica pools handle.
    pub fn pools(&self) -> Vec<&Pool> {
        self.targets.iter().map(|target| &target.pool).collect()
    }

    /// Collect all connection pools used for read queries.
    pub fn pools_with_roles_and_bans(&self) -> Vec<(Role, Ban, Pool)> {
        let result: Vec<_> = self
            .targets
            .iter()
            .map(|target| (target.role(), target.ban.clone(), target.pool.clone()))
            .collect();

        result
    }

    async fn get_internal(&self, request: &Request) -> Result<Guard, Error> {
        use LoadBalancingStrategy::*;
        use ReadWriteSplit::*;

        let mut candidates: Vec<&Target> = self
            .targets
            .iter()
            .filter(|target| !target.pool.config().resharding_only) // Don't let reads on resharding-only replicas.
            .collect();

        let primary_reads = match self.rw_split {
            IncludePrimary => true,
            IncludePrimaryIfReplicaBanned => candidates.iter().any(|target| target.ban.banned()),
            ExcludePrimary => false,
        };

        if !primary_reads {
            candidates.retain(|target| target.role() == Role::Replica);
        }

        if candidates.is_empty() {
            return Err(Error::AllReplicasDown);
        }

        match self.lb_strategy {
            Random => candidates.shuffle(&mut rand::rng()),
            RoundRobin => {
                let first = self.round_robin.fetch_add(1, Ordering::Relaxed) % candidates.len();
                let mut reshuffled = vec![];
                reshuffled.extend_from_slice(&candidates[first..]);
                reshuffled.extend_from_slice(&candidates[..first]);
                candidates = reshuffled;
            }
            LeastActiveConnections => {
                candidates.sort_by_cached_key(|target| target.pool.lock().checked_out());
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
        for target in &self.targets {
            target.pool.shutdown();
        }

        self.maintenance.notify_waiters();
    }
}
