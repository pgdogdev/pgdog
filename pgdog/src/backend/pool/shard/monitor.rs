use crate::backend::pool::lsn_monitor::ReplicaLag;

use super::*;

use futures::stream::{FuturesUnordered, StreamExt};
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
            let mut role_changes = self
                .shard
                .pool_iter()
                .map(|pool| pool.inner().lsn_role_change.notified())
                .collect::<FuturesUnordered<_>>();

            select! {
                // Updates replica lag regularly.
                _ = maintenance.tick() => {
                    if !self.shard.online() {
                        break;
                    }
                },
                // Role change needs us to run this asap.
                _ = role_changes.next() => {}
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

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use crate::backend::databases::User;
    use crate::backend::pool::lsn_monitor::LsnStats;
    use crate::backend::pool::{Address, Config, PoolConfig};
    use crate::backend::replication::publisher::Lsn;
    use crate::config::{LoadBalancingStrategy, ReadWriteSplit, Role};
    use pgdog_stats::LsnStats as StatsLsnStats;
    use tokio::time::sleep;

    use super::super::ShardConfig;
    use super::*;

    // A role-detection-enabled pool config for the given address.
    fn pool_config(address: Address) -> PoolConfig {
        PoolConfig {
            address,
            config: Config {
                inner: pgdog_stats::Config {
                    role_detection: true,
                    ..Config::default().inner
                },
            },
        }
    }

    fn set_lsn_stats(shard: &Shard, index: usize, replica: bool, lsn: i64) {
        let pools = shard.pools();
        let stats: LsnStats = StatsLsnStats {
            replica,
            lsn: Lsn::from_i64(lsn),
            offset_bytes: lsn,
            fetched: SystemTime::now(),
            ..Default::default()
        }
        .into();
        *pools[index].inner().lsn_stats.write() = stats;
    }

    // The shard monitor reacts to an `lsn_role_change` notification by
    // re-detecting roles. This confirms a simulated failover promotes the
    // replica and demotes the old primary through the live monitor loop.
    #[tokio::test]
    async fn test_monitor_updates_roles_on_failover() {
        crate::logger();

        let primary = Some(pool_config(Address::new_test()));
        let replicas = [pool_config(Address {
            configured_role: Role::Auto,
            ..Address::new_test()
        })];

        let shard = Shard::new(ShardConfig {
            number: 0,
            primary: &primary,
            replicas: &replicas,
            lb_strategy: LoadBalancingStrategy::Random,
            rw_split: ReadWriteSplit::ExcludePrimary,
            identifier: Arc::new(User {
                user: "pgdog".into(),
                database: "pgdog".into(),
            }),
            // Disable the maintenance tick (interval → MAX) so the monitor only
            // wakes on the role-change notification we fire below.
            lsn_check_interval: Duration::MAX,
            pub_sub_enabled: false,
        });

        // index 0 = replica target, index 1 = primary target.
        // Baseline: replica is behind the primary.
        set_lsn_stats(&shard, 0, true, 100);
        set_lsn_stats(&shard, 1, false, 200);

        // Launch brings pools online and spawns ShardMonitor. The per-pool LSN
        // monitors sleep for the default 5s delay, so they won't clobber the
        // stats we set during this test.
        shard.launch();

        // Initially the configured primary (index 1) holds the primary role.
        let roles = shard.pools_with_roles();
        assert_eq!(roles[1].0, Role::Primary, "configured primary is primary");
        assert_ne!(roles[0].0, Role::Primary, "replica is not the primary");

        // Simulate a failover: the replica is now ahead and out of recovery,
        // the old primary fell into recovery.
        set_lsn_stats(&shard, 0, false, 300);
        set_lsn_stats(&shard, 1, true, 200);

        // Wake the monitor as the LSN monitor would on a role change.
        shard.pools()[0].inner().lsn_role_change.notify_one();

        // Wait for the monitor to re-detect roles.
        let mut promoted = false;
        for _ in 0..40 {
            sleep(Duration::from_millis(50)).await;
            let roles = shard.pools_with_roles();
            if roles[0].0 == Role::Primary {
                assert_eq!(roles[1].0, Role::Replica, "old primary demoted");
                promoted = true;
                break;
            }
        }
        assert!(
            promoted,
            "monitor should have promoted the replica to primary"
        );

        shard.shutdown();
    }
}
