use std::time::Instant;

use super::*;

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

    async fn run(&self) {
        let mut interval = interval(MAINTENANCE);

        debug!("replicas monitor running");

        let targets = &self.replicas.targets;

        loop {
            let mut check_offline = false;
            let mut ban_targets = Vec::new();

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

            let now = Instant::now();
            let mut banned = 0;

            for (i, target) in targets.iter().enumerate() {
                let healthy = target.health.healthy();
                // Clear expired bans.
                if healthy {
                    target.ban.unban_if_expired(now);
                }

                let bannable =
                    targets.len() > 1 && target.pool.config().ban_timeout > Duration::ZERO;

                // Check health and ban if unhealthy.
                if !healthy && bannable && !target.ban.banned() {
                    ban_targets.push(i);
                    banned += 1;
                }
            }

            // Clear all bans if all targets are unhealthy.
            if targets.len() == banned {
                targets.iter().for_each(|target| {
                    target.ban.unban(false);
                });
            } else {
                for i in ban_targets {
                    targets.get(i).map(|target| {
                        target
                            .ban
                            .ban(Error::PoolUnhealthy, target.pool.config().ban_timeout)
                    });
                }
            }
        }

        debug!("replicas monitor shut down");
    }
}
