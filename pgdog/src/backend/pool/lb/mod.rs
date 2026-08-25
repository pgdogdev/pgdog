//! Load balanced connection pool.

use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use rand::seq::SliceRandom;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{config::config, net::messages::FrontendPid};
use crate::{
    config::{LoadBalancingStrategy, ReadWriteSplit, Role},
    net::Parameters,
};

use super::{Error, Guard, Oids, Pool, PoolConfig, PoolRole, Request};
use crate::util::safe_timeout;

pub mod ban;
pub mod monitor;
pub mod target_health;

use ban::Ban;
pub use ban::UnbanReason;
use monitor::*;
pub(crate) use target_health::*;

#[cfg(test)]
mod test;

/// Read query load balancer target.
#[derive(Clone, Debug)]
pub struct Target {
    pub pool: Pool,
    pub ban: Ban,
    role: PoolRole,
    /// Smooth weighted round-robin current weight tracker.
    current_weight: Arc<AtomicI64>,
}

impl Target {
    pub(super) fn new(pool: Pool, role: Role) -> Self {
        let ban = Ban::new(&pool);

        // Set pool to last known role.
        pool.set_role(role);

        Self {
            ban,
            role: PoolRole::new(role),
            pool,
            current_weight: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Get role.
    pub(super) fn role(&self) -> Role {
        self.role.role()
    }

    /// Set role.
    pub(super) fn set_role(&self, role: Role) -> bool {
        let lb = self.role.set_role(role);
        let pool = self.pool.set_role(role);

        debug_assert_eq!(
            lb, pool,
            "pool and lb role must agree: lb={lb}, pool={pool}"
        );

        lb && pool
    }

    pub(super) fn health(&self) -> &TargetHealth {
        &self.pool.inner().health
    }
}

/// Load balancer.
#[derive(Clone, Default, Debug)]
pub struct LoadBalancer {
    /// Read/write targets.
    pub(super) targets: Vec<Target>,
    /// Connection checkout timeout.
    checkout_timeout: Duration,
    /// Round robin atomic counter.
    pub(super) round_robin: Arc<AtomicUsize>,
    /// Chosen load balancing strategy.
    pub(super) lb_strategy: LoadBalancingStrategy,
    /// Shutdown signal for the replicas monitor.
    pub(super) maintenance: CancellationToken,
    /// Pool elected as primary by automatic role detection.
    pub(super) elected_primary: Arc<watch::Sender<Option<Pool>>>,
    /// Read/write split.
    pub(super) rw_split: ReadWriteSplit,
}

impl LoadBalancer {
    /// Create new replicas pools.
    pub(crate) fn new(
        primary: &Option<Pool>,
        addrs: &[PoolConfig],
        lb_strategy: LoadBalancingStrategy,
        rw_split: ReadWriteSplit,
        oids: Arc<Oids>,
    ) -> LoadBalancer {
        let checkout_timeout = primary
            .as_ref()
            .map(|pool| pool.config().checkout_timeout)
            .unwrap_or(
                addrs
                    .first()
                    .map(|addr| addr.config.checkout_timeout)
                    .unwrap_or(Duration::from_millis(
                        config().config.general.checkout_timeout,
                    )),
            );

        let mut targets: Vec<_> = addrs
            .iter()
            .map(|config| {
                let role = match config.address.configured_role {
                    Role::Auto => Role::Replica,
                    role => role,
                };
                Target::new(Pool::with_oid_mapping(config, Arc::clone(&oids)), role)
            })
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
            maintenance: CancellationToken::new(),
            elected_primary: Arc::new(watch::Sender::new(None)),
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

        self.elected_primary.send_replace(None);

        if let Some(primary) = primary {
            promoted = targets[primary].1.set_role(Role::Primary);

            if promoted {
                warn!("new primary chosen: {}", targets[primary].1.pool.addr());
            }

            // Demote everyone else to replicas.
            targets
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != primary)
                .for_each(|(_, target)| {
                    target.1.set_role(Role::Replica);
                });
        } else if targets.iter().all(|target| target.0.valid()) {
            // All targets are replicas until we get a primary.
            targets.iter().for_each(|target| {
                target.1.set_role(Role::Replica);
            });
        }

        self.elected_primary.send_replace(self.primary().cloned());

        promoted
    }

    /// Launch replica pools and start the monitor.
    pub fn launch(&self) {
        self.targets.iter().for_each(|target| target.pool.launch());
        Monitor::spawn(self);
    }

    /// Check that the load balancer targets are all launched.
    pub fn online(&self) -> bool {
        self.targets.iter().all(|target| target.pool.lock().online)
    }

    /// Get a live connection from the pool.
    pub async fn get(&self, request: &Request) -> Result<Guard, Error> {
        self.get_internal(request).await
    }

    /// Get parameters from first non-banned connection pool.
    pub async fn params(&self, request: &Request) -> Result<&Parameters, Error> {
        if let Some(target) = self.targets.iter().find(|target| !target.ban.banned()) {
            return target.pool.params(request).await;
        }

        Err(Error::AllReplicasDown)
    }

    /// Move connections from this replica set to another.
    ///
    /// Uses address-based matching so existing pools survive replica additions or
    /// removals: each old target is paired with the new target that shares its
    /// address. New targets with no matching old target start empty; old targets
    /// with no match in the new config have their connections dropped.
    pub fn move_conns_to(&self, destination: &LoadBalancer) -> Result<(), Error> {
        for from in &self.targets {
            if let Some(to) = destination
                .targets
                .iter()
                .find(|to| from.pool.has_compatible_address_with(&to.pool))
            {
                from.pool.move_conns_to(&to.pool)?;

                // Carry over detected roles and LSN stats so the new load balancer
                // doesn't briefly appear read-only before the role detector runs.
                to.set_role(from.role());
                *to.pool.inner().lsn_stats.write() = from.pool.lsn_stats();
            }
        }
        destination.require_healthcheck_for_new_targets(&self.targets);

        Ok(())
    }

    /// The two replica sets are referring to the same databases.
    ///
    /// Returns `true` when every target in `self` has a matching address in
    /// `destination`. This allows replica additions (new targets start empty)
    /// while still preserving connections to unchanged replicas.
    pub fn can_move_conns_to(&self, destination: &LoadBalancer) -> bool {
        self.targets.iter().all(|from| {
            destination
                .targets
                .iter()
                .any(|to| from.pool.has_compatible_address_with(&to.pool))
        })
    }

    /// True if the LB has any target that can serve replica reads.
    pub fn has_replicas(&self) -> bool {
        self.targets
            .iter()
            .any(|target| target.role() == Role::Replica)
    }

    /// True if target roles are detected automatically.
    pub fn role_detection_enabled(&self) -> bool {
        !self.targets.is_empty()
            && self
                .targets
                .iter()
                .all(|target| target.pool.config().role_detection)
    }

    /// Cancel a query if one is running.
    pub async fn cancel(&self, id: FrontendPid) -> Result<(), super::super::Error> {
        for target in &self.targets {
            target.pool.cancel(id).await?;
        }

        Ok(())
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

    /// Check out a connection from the primary.
    ///
    /// In automatic mode, the caller waits until role detection elects a
    /// primary, or until the checkout times out. Static replica-only
    /// configurations fail immediately with [`Error::NoPrimary`].
    pub(super) async fn get_primary(&self, request: &Request) -> Result<Guard, Error> {
        if let Some(pool) = self.primary() {
            return pool.get(request).await;
        }

        if !self.role_detection_enabled() {
            return Err(Error::NoPrimary);
        }

        let mut receiver = self.elected_primary.subscribe();

        let primary = safe_timeout(self.checkout_timeout, receiver.wait_for(|p| p.is_some()))
            .await
            .map_err(|_| Error::CheckoutTimeout)?
            .ok()
            .and_then(|elected| elected.as_ref().cloned())
            .ok_or(Error::NoPrimary)?;

        primary.get(request).await
    }

    async fn get_internal(&self, request: &Request) -> Result<Guard, Error> {
        use LoadBalancingStrategy::*;
        use ReadWriteSplit::*;
        use smallvec::SmallVec;

        let mut candidates: SmallVec<[&Target; 32]> = self
            .targets
            .iter()
            .filter(|target| !target.pool.config().resharding_only) // Don't let reads on resharding-only replicas.
            .collect();

        let has_unbanned_replica = candidates
            .iter()
            .any(|target| target.role() == Role::Replica && !target.ban.banned());
        // If `read_only` is true, only allow if there's no replicas.
        let primary_reads = match self.rw_split {
            IncludePrimary => !(request.read_only && has_unbanned_replica),
            IncludePrimaryIfReplicaBanned => {
                !(request.read_only && has_unbanned_replica)
                    && (candidates.iter().any(|target| target.ban.banned())
                        || candidates.len() == 1) // The second condition is for when there is only the primary (just one target, doesn't matter what role it is).
            }
            // we read from the primary if we have no replicas
            ExcludePrimary => !has_unbanned_replica,
            // PreferPrimary makes all queries writes. If a query lands here,
            // it's because of pgdog.role=replica. Let it use the primary only if
            // no replicas are available.
            PreferPrimary => !has_unbanned_replica,
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
                let mut reshuffled = SmallVec::with_capacity(candidates.len());
                reshuffled.extend_from_slice(&candidates[first..]);
                reshuffled.extend_from_slice(&candidates[..first]);
                candidates = reshuffled;
            }
            LeastActiveConnections => {
                candidates.sort_by_cached_key(|target| target.pool.lock().checked_out());
            }
            WeightedRoundRobin => {
                let total_weight: i64 = candidates
                    .iter()
                    .map(|target| target.pool.config().lb_weight as i64)
                    .sum();

                if total_weight > 0 {
                    for target in &candidates {
                        target
                            .current_weight
                            .fetch_add(target.pool.config().lb_weight as i64, Ordering::Relaxed);
                    }

                    let max_idx = candidates
                        .iter()
                        .enumerate()
                        .max_by_key(|(_, t)| t.current_weight.load(Ordering::Relaxed))
                        .map(|(idx, _)| idx)
                        .unwrap_or_default();

                    candidates[max_idx]
                        .current_weight
                        .fetch_sub(total_weight, Ordering::Relaxed);

                    candidates.swap(0, max_idx);
                }
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

        candidates
            .iter()
            .for_each(|target| target.ban.unban(true, UnbanReason::AllTargetsBanned));

        Err(Error::AllReplicasDown)
    }

    /// Shutdown replica pools.
    ///
    /// N.B. The primary pool is managed by `super::Shard`.
    pub fn shutdown(&self) {
        for target in &self.targets {
            target.pool.shutdown();
        }

        self.maintenance.cancel();
    }

    fn require_healthcheck_for_new_targets(&self, old_targets: &[Target]) {
        for target in &self.targets {
            let old_target = old_targets
                .iter()
                .find(|t| t.pool.has_compatible_address_with(&target.pool));

            if let Some(old) = old_target
                && let Some(Error::InitialHealthCheck) = old.ban.error()
            {
                target.ban.ban(Error::InitialHealthCheck, Duration::ZERO);
                target.health().toggle(old.health().healthy());
            } else if target.pool.config().require_healthcheck_on_discovery {
                target.ban.ban(Error::InitialHealthCheck, Duration::ZERO);
                target.health().toggle(false);
            }
        }
    }
}
