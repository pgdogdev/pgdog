//! Replicas pool.

use std::time::Duration;

use rand::seq::IteratorRandom;
use tokio::time::timeout;

use crate::net::messages::BackendKeyData;

use super::{Error, Guard, Pool};

/// Replicas pools.
pub struct Replicas {
    pools: Vec<Pool>,
    checkout_timeout: Duration,
}

impl Replicas {
    /// Create new replicas pools.
    pub fn new(addrs: &[&str]) -> Replicas {
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

    async fn get_internal(&self, id: &BackendKeyData) -> Result<Guard, Error> {
        loop {
            if self.is_empty() {
                return Err(Error::NoReplicas);
            }

            let clear = self
                .pools
                .iter()
                .filter(|p| p.available())
                .choose(&mut rand::thread_rng());

            if let Some(clear) = clear {
                match clear.get(id).await {
                    Ok(conn) => return Ok(conn),
                    Err(_err) => {
                        clear.ban();
                    }
                }
            } else {
                self.pools.iter().for_each(|p| p.unban());
            }
        }
    }
}
