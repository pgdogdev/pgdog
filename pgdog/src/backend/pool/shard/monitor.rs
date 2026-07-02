use crate::backend::pool::lsn_monitor::{LsnStats, ReplicaLag};

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

            update_replica_lag(&self.shard.pools());
        }

        debug!(
            "shard {} monitor shutdown [{}]",
            self.shard.number(),
            self.shard.identifier()
        );
    }
}

fn update_replica_lag(pools: &[Pool]) {
    let primary = pools
        .iter()
        .map(|pool| (pool, pool.lsn_stats()))
        .find(|(_, stats)| !stats.replica);

    // There is a primary. If not, replica lag cannot be calculated.
    if let Some((primary_pool, primary_stats)) = primary {
        for replica_pool in pools {
            let replica_stats = replica_pool.lsn_stats();
            if !replica_stats.replica {
                continue;
            }

            let lag = calculate_replica_lag(&primary_stats, &replica_stats);
            replica_pool.lock().replica_lag = lag;
        }
        primary_pool.lock().replica_lag = ReplicaLag::default();
    }
}

fn calculate_replica_lag(primary: &LsnStats, replica: &LsnStats) -> ReplicaLag {
    debug_assert!(
        !primary.replica,
        "primary stats must come from the primary pool"
    );
    debug_assert!(
        replica.replica,
        "replica stats must come from the replica pool"
    );

    // Replica lag is how far the replica trails the primary.
    let bytes = primary.lsn.lsn - replica.lsn.lsn;
    if bytes <= 0 {
        return ReplicaLag::default();
    }

    let lag_ms = (primary.timestamp.to_naive_datetime() - replica.timestamp.to_naive_datetime())
        .num_milliseconds()
        .clamp(0, i64::MAX);

    ReplicaLag {
        bytes,
        duration: Duration::from_millis(lag_ms as u64),
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
    use pgdog_postgres_types::{Format, FromDataType, TimestampTz};
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

    fn set_pool_lsn_stats(pool: &Pool, replica: bool, lsn: i64, timestamp: &str) {
        *pool.inner().lsn_stats.write() = lsn_stats(replica, lsn, timestamp);
    }

    fn lsn_stats(replica: bool, lsn: i64, timestamp: &str) -> LsnStats {
        StatsLsnStats {
            replica,
            lsn: Lsn::from_i64(lsn),
            offset_bytes: lsn,
            timestamp: TimestampTz::decode(timestamp.as_bytes(), Format::Text).unwrap(),
            fetched: SystemTime::now(),
            aurora: false,
        }
        .into()
    }

    // Regression test for the shard monitor lag calculation. The broken monitor
    // called `primary.replica_lag(replica)`, which inverted bytes and clamped the
    // duration to zero, preventing replica-lag bans from triggering.
    #[test]
    fn test_calculate_replica_lag_uses_replica_against_primary() {
        let primary = lsn_stats(false, 200, "2026-07-01 13:33:10.000000+00");
        let replica = lsn_stats(true, 100, "2026-07-01 13:33:00.000000+00");

        let lag = calculate_replica_lag(&primary, &replica);

        assert_eq!(lag.bytes, 100);
        assert_eq!(lag.duration.as_secs(), 10);
    }

    #[test]
    fn test_calculate_replica_lag_is_zero_when_replica_is_not_behind() {
        let primary = lsn_stats(false, 100, "2026-07-01 13:33:00.000000+00");
        let replica = lsn_stats(true, 200, "2026-07-01 13:33:10.000000+00");

        let lag = calculate_replica_lag(&primary, &replica);

        assert_eq!(lag.bytes, 0);
        assert_eq!(lag.duration, Duration::default());
    }

    #[test]
    #[should_panic(expected = "primary stats must come from the primary pool")]
    fn test_calculate_replica_lag_rejects_swapped_stats() {
        let primary = lsn_stats(false, 200, "2026-07-01 13:33:10.000000+00");
        let replica = lsn_stats(true, 100, "2026-07-01 13:33:00.000000+00");

        let _ = calculate_replica_lag(&replica, &primary);
    }

    #[test]
    fn test_update_replica_lag_assigns_primary_minus_replica_to_replica_pool() {
        let primary = Pool::new(&PoolConfig {
            address: Address::new_test(),
            config: Config::default(),
        });
        let replica = Pool::new(&PoolConfig {
            address: Address {
                configured_role: Role::Replica,
                ..Address::new_test()
            },
            config: Config::default(),
        });

        set_pool_lsn_stats(&primary, false, 200, "2026-07-01 13:33:10.000000+00");
        set_pool_lsn_stats(&replica, true, 100, "2026-07-01 13:33:00.000000+00");

        update_replica_lag(&[replica.clone(), primary.clone()]);

        let replica_lag = replica.replica_lag();
        assert_eq!(replica_lag.bytes, 100);
        assert_eq!(replica_lag.duration.as_secs(), 10);

        let primary_lag = primary.replica_lag();
        assert_eq!(primary_lag.bytes, 0);
        assert_eq!(primary_lag.duration, Duration::default());
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
