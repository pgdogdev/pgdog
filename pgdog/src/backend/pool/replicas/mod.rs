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
use tracing::error;

use crate::config::{LoadBalancingStrategy, Role};
use crate::net::messages::BackendKeyData;

use super::{Error, Guard, Pool, PoolConfig, Request};

pub mod ban;
pub mod monitor;
use ban::Ban;
use monitor::*;

/// Replicas pools.
#[derive(Clone, Default, Debug)]
pub struct Replicas {
    /// Connection pools.
    pub(super) replica_pools: Vec<Pool>,
    /// Primary pool.
    pub primary: Option<Pool>,
    /// Bans.
    pub(super) replica_bans: Vec<Ban>,
    /// Primary ban.
    pub(super) primary_read_ban: Option<Ban>,
    /// Checkout timeout.
    pub(super) checkout_timeout: Duration,
    /// Round robin atomic counter.
    pub(super) round_robin: Arc<AtomicUsize>,
    /// Chosen load balancing strategy.
    pub(super) lb_strategy: LoadBalancingStrategy,
    /// Shutdown notification.
    pub(super) shutdown: Arc<Notify>,
}

impl Replicas {
    /// Create new replicas pools.
    pub fn new(
        primary: &Option<Pool>,
        addrs: &[PoolConfig],
        lb_strategy: LoadBalancingStrategy,
    ) -> Replicas {
        let checkout_timeout = addrs
            .iter()
            .map(|c| c.config.checkout_timeout())
            .sum::<Duration>();

        let replica_pools: Vec<_> = addrs.iter().map(Pool::new).collect();

        let replicas = Self {
            primary: primary.clone(),
            replica_bans: replica_pools.iter().map(|pool| Ban::new(pool)).collect(),
            replica_pools,
            primary_read_ban: primary.as_ref().map(|pool| Ban::new(&pool)),
            checkout_timeout,
            round_robin: Arc::new(AtomicUsize::new(0)),
            lb_strategy,
            shutdown: Arc::new(Notify::new()),
        };

        Monitor::new(&replicas);

        replicas
    }

    /// Get a live connection from the pool.
    pub async fn get(&self, request: &Request, primary_reads: bool) -> Result<Guard, Error> {
        match timeout(
            self.checkout_timeout,
            self.get_internal(request, primary_reads),
        )
        .await
        {
            Ok(Ok(conn)) => Ok(conn),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(Error::ReplicaCheckoutTimeout),
        }
    }

    /// Move connections from this replica set to another.
    pub fn move_conns_to(&self, destination: &Replicas) {
        assert_eq!(self.replica_pools.len(), destination.replica_pools.len());

        for (from, to) in self
            .replica_pools
            .iter()
            .zip(destination.replica_pools.iter())
        {
            from.move_conns_to(to);
        }
    }

    /// The two replica sets are referring to the same databases.
    pub fn can_move_conns_to(&self, destination: &Replicas) -> bool {
        self.replica_pools.len() == destination.replica_pools.len()
            && self
                .replica_pools
                .iter()
                .zip(destination.replica_pools.iter())
                .all(|(a, b)| a.can_move_conns_to(b))
    }

    /// How many replicas we are connected to.
    pub fn len(&self) -> usize {
        self.replica_pools.len()
    }

    /// There are no replicas.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create new identical replica pool.
    pub fn duplicate(&self) -> Replicas {
        Self {
            replica_pools: self.replica_pools.iter().map(|p| p.duplicate()).collect(),
            primary: self.primary.clone(),
            replica_bans: self.replica_bans.clone(),
            primary_read_ban: self.primary_read_ban.clone(),
            checkout_timeout: self.checkout_timeout,
            round_robin: Arc::new(AtomicUsize::new(0)),
            lb_strategy: self.lb_strategy,
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Cancel a query if one is running.
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        for pool in &self.replica_pools {
            pool.cancel(id).await?;
        }

        Ok(())
    }

    /// Replica pools handle.
    pub fn pools(&self) -> &[Pool] {
        &self.replica_pools
    }

    pub fn pools_with_roles_and_bans(&self) -> Vec<(Role, Ban, Pool)> {
        let mut replicas: Vec<_> = self
            .replica_pools
            .iter()
            .zip(self.replica_bans.iter())
            .map(|(pool, ban)| (Role::Replica, ban.clone(), pool.clone()))
            .collect();

        if let Some(ref primary) = self.primary {
            if let Some(ref ban) = self.primary_read_ban {
                replicas.push((Role::Primary, ban.clone(), primary.clone()));
            }
        }

        replicas
    }

    async fn get_internal(&self, request: &Request, primary_reads: bool) -> Result<Guard, Error> {
        let mut unbanned = false;
        loop {
            let mut candidates = self
                .replica_pools
                .iter()
                .zip(self.replica_bans.iter())
                .collect::<Vec<_>>();

            if primary_reads {
                if let Some(primary) = self.primary.as_ref() {
                    if let Some(ref ban) = self.primary_read_ban {
                        candidates.push((primary, ban));
                    }
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
                    candidates.sort_by_cached_key(|(pool, _banned)| pool.lock().idle());
                }
            }

            let mut banned = 0;

            for (candidate, ban) in &candidates {
                if ban.banned() {
                    banned += 1;
                    continue;
                }
                match candidate.get(request).await {
                    Ok(conn) => return Ok(conn),
                    Err(Error::Offline) => continue,
                    Err(err) => {
                        // Only ban a candidate pool if there are more than one
                        // and we have alternates.
                        if candidates.len() > 1 {
                            banned += 1;

                            // Only log the ban once.
                            let ban_timeout = candidate.config().ban_timeout;
                            if ban.ban(err, ban_timeout) {
                                error!(
                                    "read queries ban due to pool error: {} [{}]",
                                    err,
                                    candidate.addr()
                                );
                            }
                        }
                    }
                }
            }

            // All replicas are banned, unban everyone.
            if banned == candidates.len() && !unbanned {
                candidates.iter().for_each(|(_pool, ban)| ban.unban());
                unbanned = true;
            } else {
                break;
            }
        }

        Err(Error::AllReplicasDown)
    }
}
