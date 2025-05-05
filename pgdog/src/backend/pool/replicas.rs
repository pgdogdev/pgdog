//! Replicas pool.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use rand::seq::SliceRandom;
use tokio::time::timeout;
use tracing::error;

use crate::config::LoadBalancingStrategy;
use crate::net::messages::BackendKeyData;

use super::{Error, Guard, Pool, PoolConfig, Request};

/// Replicas pools.
#[derive(Clone, Default, Debug)]
pub struct Replicas {
    /// Connection pools.
    pub(super) pools: Vec<Pool>,
    /// Checkout timeout.
    pub(super) checkout_timeout: Duration,
    /// Round robin atomic counter.
    pub(super) round_robin: Arc<AtomicUsize>,
    /// Chosen load balancing strategy.
    pub(super) lb_strategy: LoadBalancingStrategy,
}

impl Replicas {
    /// Create new replicas pools.
    pub fn new(addrs: &[PoolConfig], lb_strategy: LoadBalancingStrategy) -> Replicas {
        let checkout_timeout = addrs
            .iter()
            .map(|c| c.config.checkout_timeout())
            .sum::<Duration>();
        Self {
            pools: addrs.iter().map(Pool::new).collect(),
            checkout_timeout,
            round_robin: Arc::new(AtomicUsize::new(0)),
            lb_strategy,
        }
    }

    /// Get a live connection from the pool.
    pub async fn get(&self, request: &Request, primary: &Option<Pool>) -> Result<Guard, Error> {
        match timeout(self.checkout_timeout, self.get_internal(request, primary)).await {
            Ok(Ok(conn)) => Ok(conn),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(Error::ReplicaCheckoutTimeout),
        }
    }

    /// Move connections from this replica set to another.
    pub fn move_conns_to(&self, destination: &Replicas) {
        assert_eq!(self.pools.len(), destination.pools.len());

        for (from, to) in self.pools.iter().zip(destination.pools.iter()) {
            from.move_conns_to(to);
        }
    }

    /// The two replica sets are referring to the same databases.
    pub fn can_move_conns_to(&self, destination: &Replicas) -> bool {
        self.pools.len() == destination.pools.len()
            && self
                .pools
                .iter()
                .zip(destination.pools.iter())
                .all(|(a, b)| a.can_move_conns_to(b))
    }

    /// How many replicas we are connected to.
    pub fn len(&self) -> usize {
        self.pools.len()
    }

    /// There are no replicas.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create new identical replica pool.
    pub fn duplicate(&self) -> Replicas {
        Self {
            pools: self.pools.iter().map(|p| p.duplicate()).collect(),
            checkout_timeout: self.checkout_timeout,
            round_robin: Arc::new(AtomicUsize::new(0)),
            lb_strategy: self.lb_strategy,
        }
    }

    /// Cancel a query if one is running.
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        for pool in &self.pools {
            pool.cancel(id).await?;
        }

        Ok(())
    }

    /// Pools handle.
    pub fn pools(&self) -> &[Pool] {
        &self.pools
    }

    async fn get_internal(
        &self,
        request: &Request,
        primary: &Option<Pool>,
    ) -> Result<Guard, Error> {
        let mut unbanned = false;
        loop {
            let mut candidates = self.pools.iter().collect::<Vec<_>>();

            if let Some(primary) = primary {
                candidates.push(primary);
            }

            match self.lb_strategy {
                LoadBalancingStrategy::Random => candidates.shuffle(&mut rand::thread_rng()),
                LoadBalancingStrategy::RoundRobin => {
                    let first = self.round_robin.fetch_add(1, Ordering::Relaxed) % candidates.len();
                    let mut reshuffled = vec![];
                    reshuffled.extend_from_slice(&candidates[first..]);
                    reshuffled.extend_from_slice(&candidates[..first]);
                    candidates = reshuffled;
                }
                LoadBalancingStrategy::LeastActiveConnections => {
                    candidates.sort_by_cached_key(|pool| pool.lock().idle());
                }
            }

            let mut banned = 0;

            for candidate in &candidates {
                match candidate.get(request).await {
                    Ok(conn) => return Ok(conn),
                    Err(Error::Offline) => continue,
                    Err(Error::Banned) => {
                        banned += 1;
                        continue;
                    }
                    Err(err) => {
                        error!("{} [{}]", err, candidate.addr());
                    }
                }
            }

            // All replicas are banned, unban everyone.
            if banned == candidates.len() && !unbanned {
                candidates.iter().for_each(|candidate| candidate.unban());
                unbanned = true;
            } else {
                break;
            }
        }

        Err(Error::AllReplicasDown)
    }
}
