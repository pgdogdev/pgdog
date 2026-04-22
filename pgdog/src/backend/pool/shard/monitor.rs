use crate::backend::pool::lsn_monitor::ReplicaLag;

use super::*;

use tokio::time::interval;
use tracing::{info, warn};

/// Shard communication primitives.
#[derive(Debug)]
pub(super) struct ShardComms {
    pub(super) shutdown: Notify,
    pub(super) lsn_check_interval: Duration,
}

impl Default for ShardComms {
    fn default() -> Self {
        Self {
            shutdown: Notify::new(),
            lsn_check_interval: Duration::MAX,
        }
    }
}

/// Monitor shard connection pools for various stats.
///
/// Currently, only checking for replica lag, if any replicas are configured
/// and this is enabled.
pub(super) struct ShardMonitor {
    shard: Shard,
}

impl ShardMonitor {
    /// Run the shard monitor.
    pub(super) fn run(shard: &Shard) {
        let monitor = Self {
            shard: shard.clone(),
        };

        spawn(async move { monitor.spawn().await });
    }
}

impl ShardMonitor {
    async fn spawn(&self) {
        let maintenance = (self.shard.comms().lsn_check_interval / 2)
            .clamp(Duration::from_millis(333), Duration::MAX);
        let mut maintenance = interval(maintenance);

        debug!(
            "shard {} monitor running [{}]",
            self.shard.number(),
            self.shard.identifier()
        );

        let mut detector = RoleDetector::new(&self.shard);

        if detector.enabled() {
            info!(
                "automatic database role detection is enabled for shard {} [{}]",
                self.shard.number(),
                self.shard.identifier()
            );
        }

        loop {
            select! {
                _ = maintenance.tick() => {},
                _ = self.shard.comms().shutdown.notified() => {
                    break;
                },
            }

            if detector.changed() {
                warn!(
                    "database role changed in shard {} [{}]",
                    self.shard.number(),
                    self.shard.identifier()
                );
            }

            let pool_with_stats = self
                .shard
                .pools()
                .iter()
                .map(|pool| (pool.clone(), pool.lsn_stats()))
                .collect::<Vec<_>>();

            let primary = pool_with_stats.iter().find(|pair| !pair.1.replica);

            // There is a primary. If not, replica lag cannot be
            // calculated.
            if let Some(primary) = primary {
                let replicas = pool_with_stats.iter().filter(|pair| pair.1.replica);
                for replica in replicas {
                    // Primary is ahead, there is replica lag.
                    let lag = if primary.1.lsn.lsn > replica.1.lsn.lsn {
                        primary.1.replica_lag(&replica.1)
                    } else {
                        ReplicaLag::default()
                    };
                    replica.0.lock().replica_lag = lag;
                }
                primary.0.lock().replica_lag = ReplicaLag::default();
            }
        }

        debug!(
            "shard {} monitor shutdown [{}]",
            self.shard.number(),
            self.shard.identifier()
        );
    }
}
