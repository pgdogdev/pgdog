//! Replicas pool.

use std::time::Duration;

use rand::seq::SliceRandom;
use tokio::time::timeout;
use tracing::error;

use crate::net::messages::BackendKeyData;

use super::{Error, Guard, Pool, PoolConfig};

/// Replicas pools.
#[derive(Clone)]
pub struct Replicas {
    pools: Vec<Pool>,
    checkout_timeout: Duration,
}

impl Replicas {
    /// Create new replicas pools.
    pub fn new(addrs: &[PoolConfig]) -> Replicas {
        Self {
            pools: addrs.iter().map(|p| Pool::new(p.clone())).collect(),
            checkout_timeout: Duration::from_millis(5_000),
        }
    }

    /// Get a live connection from the pool.
    pub async fn get(&self, id: &BackendKeyData, primary: &Option<Pool>) -> Result<Guard, Error> {
        match timeout(
            self.checkout_timeout * self.pools.len() as u32,
            self.get_internal(id, primary),
        )
        .await
        {
            Ok(Ok(conn)) => Ok(conn),
            _ => Err(Error::CheckoutTimeout),
        }
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
        id: &BackendKeyData,
        primary: &Option<Pool>,
    ) -> Result<Guard, Error> {
        let mut candidates = self
            .pools
            .iter()
            .map(|pool| (pool.banned(), pool))
            .collect::<Vec<_>>();

        if let Some(primary) = primary {
            candidates.push((primary.banned(), primary));
        }

        candidates.shuffle(&mut rand::thread_rng());

        // All replicas are banned, unban everyone.
        let banned = candidates.iter().all(|(banned, _)| *banned);
        if banned {
            candidates
                .iter()
                .for_each(|(_, candidate)| candidate.unban());
        }

        for (_, candidate) in &candidates {
            match candidate.get(id).await {
                Ok(conn) => return Ok(conn),
                Err(Error::Offline) => continue,
                Err(err) => {
                    error!("{} [{}]", err, candidate.addr());
                }
            }
        }

        Err(Error::Offline)
    }
}
