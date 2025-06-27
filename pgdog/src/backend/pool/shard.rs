//! A shard is a collection of replicas and a primary.

use crate::{
    config::{LoadBalancingStrategy, ReadWriteSplit, Role},
    net::messages::BackendKeyData,
};
use tokio::{spawn, time::interval};
use tracing::{debug, error};

use super::{Error, Guard, Pool, PoolConfig, Replicas, Request, ReplicationLagChecker};

/// Primary and replicas.
#[derive(Clone, Default, Debug)]
pub struct Shard {
    pub(super) primary: Option<Pool>,
    pub(super) replicas: Replicas,
    pub(super) rw_split: ReadWriteSplit,
}

impl Shard {
    /// Create new shard connection pool.
    pub fn new(
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

    /// Get a connection to the shard primary database.
    pub async fn primary(&self, request: &Request) -> Result<Guard, Error> {
        self.primary
            .as_ref()
            .ok_or(Error::NoPrimary)?
            .get_forced(request)
            .await
    }

    /// Get a connection to a shard replica, if any.
    pub async fn replica(&self, request: &Request) -> Result<Guard, Error> {
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

    /// Move pool connections from self to destination.
    /// This shuts down my pool.
    pub fn move_conns_to(&self, destination: &Shard) {
        if let Some(ref primary) = self.primary {
            if let Some(ref other) = destination.primary {
                primary.move_conns_to(other);
            }
        }

        self.replicas.move_conns_to(&destination.replicas);
    }

    /// The two shards have the same databases.
    pub(crate) fn can_move_conns_to(&self, other: &Shard) -> bool {
        if let Some(ref primary) = self.primary {
            if let Some(ref other) = other.primary {
                if !primary.can_move_conns_to(other) {
                    return false;
                }
            } else {
                return false;
            }
        }

        self.replicas.can_move_conns_to(&other.replicas)
    }

    /// Create new identical connection pool.
    pub fn duplicate(&self) -> Self {
        Self {
            primary: self.primary.as_ref().map(|primary| primary.duplicate()),
            replicas: self.replicas.duplicate(),
            rw_split: self.rw_split,
        }
    }

    /// Cancel a query if one is running.
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        if let Some(ref primary) = self.primary {
            primary.cancel(id).await?;
        }
        self.replicas.cancel(id).await?;

        Ok(())
    }

    /// Get all pools. Used for administrative tasks.
    pub fn pools(&self) -> Vec<Pool> {
        self.pools_with_roles()
            .into_iter()
            .map(|(_, pool)| pool)
            .collect()
    }

    pub fn pools_with_roles(&self) -> Vec<(Role, Pool)> {
        let mut pools = vec![];
        if let Some(primary) = self.primary.clone() {
            pools.push((Role::Primary, primary));
        }
        pools.extend(
            self.replicas
                .pools()
                .iter()
                .map(|p| (Role::Replica, p.clone())),
        );

        pools
    }

    /// Launch the shard, bringing all pools online.
    pub fn launch(&self) {
        self.pools().iter().for_each(|pool| pool.launch());
        
        // Launch replication lag monitoring if we have both primary and replicas
        if let Some(primary) = &self.primary {
            if !self.replicas.is_empty() {
                Self::launch_lag_monitoring(primary.clone(), self.replicas.clone());
            }
        }
    }

    /// Shutdown all pools, taking the shard offline.
    pub fn shutdown(&self) {
        self.pools().iter().for_each(|pool| pool.shutdown());
    }

    /// Launch replication lag monitoring task.
    fn launch_lag_monitoring(primary: Pool, replicas: Replicas) {
        spawn(async move {
            Self::replication_lag_monitor(primary, replicas).await;
        });
    }

    /// Monitor replication lag and ban lagging replicas.
    async fn replication_lag_monitor(primary: Pool, replicas: Replicas) {
        debug!("Starting replication lag monitoring");

        // Get configuration from one of the replica pools
        let (check_interval, max_lag_bytes) = {
            if let Some(pool) = replicas.pools.first() {
                let config = pool.lock().config;
                (config.replication_lag_check_interval(), config.max_replication_lag_bytes())
            } else {
                return; // No replicas to monitor
            }
        };

        let mut interval = interval(check_interval);

        loop {
            // Wait for next check interval
            interval.tick().await;

            // Check if primary is online
            {
                let primary_guard = primary.lock();
                if !primary_guard.online {
                    debug!("Primary is offline, stopping replication lag monitoring");
                    break;
                }
            }

            // Perform replication lag check
            let checker = ReplicationLagChecker::new(&primary, &replicas, max_lag_bytes);
            if let Err(err) = checker.check_and_ban_lagging_replicas().await {
                error!("Replication lag check failed: {}", err);
                // Continue monitoring even if this check failed
            }
        }

        debug!("Replication lag monitoring stopped");
    }
}

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

        let shard = Shard::new(
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

        let shard = Shard::new(
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
