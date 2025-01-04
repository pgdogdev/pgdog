//! Replicas pool.

use std::time::Duration;

use rand::seq::IteratorRandom;
use tokio::time::timeout;
use tracing::error;

use crate::net::messages::BackendKeyData;

use super::{Address, Error, Guard, Pool};

/// Replicas pools.
#[derive(Clone)]
pub struct Replicas {
    pools: Vec<Pool>,
    checkout_timeout: Duration,
}

impl Replicas {
    /// Create new replicas pools.
    pub fn new(addrs: &[&Address]) -> Replicas {
        Self {
            pools: addrs.iter().map(|p| Pool::new(p)).collect(),
            checkout_timeout: Duration::from_millis(5_000),
        }
    }

    /// Get a live connection from the pool.
    pub async fn get(&self, id: &BackendKeyData) -> Result<Guard, Error> {
        match timeout(self.checkout_timeout, self.get_internal(id)).await {
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

    async fn get_internal(&self, id: &BackendKeyData) -> Result<Guard, Error> {
        loop {
            if self.is_empty() {
                return Err(Error::NoReplicas);
            }

            let candidate = self
                .pools
                .iter()
                .filter(|pool| pool.available())
                .choose(&mut rand::thread_rng());

            if let Some(candidate) = candidate {
                match candidate.get(id).await {
                    Ok(conn) => return Ok(conn),
                    Err(err) => {
                        candidate.ban();
                        error!("{}", err);
                    }
                }
            } else {
                self.pools.iter().for_each(|p| p.unban());
            }
        }
    }
}
