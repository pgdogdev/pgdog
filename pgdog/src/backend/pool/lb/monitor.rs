use std::time::Instant;

use crate::config::config;

use super::*;

use pgdog_stats::ReplicaLag;
use tokio::{select, spawn, task::JoinHandle, time::interval};
use tracing::debug;

static MAINTENANCE: Duration = Duration::from_millis(333);

#[derive(Clone, Debug)]
pub(super) struct Monitor {
    replicas: LoadBalancer,
}

impl Monitor {
    /// Create new replica targets monitor.
    pub(super) fn spawn(replicas: &LoadBalancer) -> JoinHandle<()> {
        let monitor = Self {
            replicas: replicas.clone(),
        };

        spawn(async move {
            monitor.run().await;
        })
    }

    /// Create a Monitor instance for testing.
    #[cfg(test)]
    pub(super) fn new_test(replicas: &LoadBalancer) -> Self {
        Self {
            replicas: replicas.clone(),
        }
    }

    async fn run(&self) {
        let mut interval = interval(MAINTENANCE);

        debug!("replicas monitor running");
        let config = config();

        let replica_ban_threshold = ReplicaLag {
            duration: Duration::from_millis(config.config.general.ban_replica_lag),
            bytes: config.config.general.ban_replica_lag_bytes as i64,
        };

        loop {
            let mut check_offline = false;

            select! {
                _ = interval.tick() => {}
                _ = self.replicas.maintenance.notified() => {
                    check_offline = true;
                }
            }

            if check_offline {
                let offline = self
                    .replicas
                    .targets
                    .iter()
                    .all(|target| !target.pool.lock().online);

                if offline {
                    break;
                }
            }

            self.ban_check(&replica_ban_threshold);
        }

        debug!("replicas monitor shut down");
    }

    /// Check for unhealthy targets and ban them, or clear expired bans.
    /// This is pub(super) to enable testing.
    pub(super) fn ban_check(&self, replica_ban_threshold: &ReplicaLag) {
        let now = Instant::now();
        let mut banned = 0;
        let mut ban_targets = Vec::new();
        let targets = &self.replicas.targets;

        for (i, target) in targets.iter().enumerate() {
            let healthy = target.health.healthy();
            let replica_lag_bad = target
                .pool
                .state()
                .replica_lag
                .greater_or_eq(replica_ban_threshold);

            // Clear expired bans.
            if healthy && !replica_lag_bad {
                target.ban.unban_if_expired(now);
            }

            let bannable = targets.len() > 1 && target.pool.config().ban_timeout > Duration::ZERO;

            // Check health and ban if unhealthy.
            if !healthy && bannable {
                let already_banned = target.ban.banned();
                if already_banned || !healthy {
                    banned += 1;
                }
                if !healthy {
                    let reason = if replica_lag_bad {
                        Error::ReplicaLag
                    } else {
                        Error::PoolUnhealthy
                    };

                    ban_targets.push((i, reason));
                }
            }
        }

        // Clear all bans if all targets are unhealthy.
        if targets.len() == banned {
            targets.iter().for_each(|target| {
                target.ban.unban(false);
            });
        } else {
            for (i, reason) in ban_targets {
                targets
                    .get(i)
                    .map(|target| target.ban.ban(reason, target.pool.config().ban_timeout));
            }
        }
    }
}
