use super::Shard;

pub(super) struct RoleDetector {
    enabled: bool,
    primary_id: Option<u64>,
    shard: Shard,
}

impl RoleDetector {
    /// Create new role change detector.
    pub(super) fn new(shard: &Shard) -> Self {
        let primary_id = shard.lb.primary().map(|pool| pool.id());
        Self {
            enabled: shard
                .pools()
                .iter()
                .all(|pool| pool.config().role_detection),
            primary_id,
            shard: shard.clone(),
        }
    }

    /// Detect role change in the shard.
    pub(super) fn changed(&mut self) -> bool {
        if self.enabled() {
            let lb_changed = self.shard.redetect_roles();
            let primary_id = self.shard.lb.primary().map(|pool| pool.id());
            let changed = lb_changed || primary_id != self.primary_id;
            if changed {
                // Re-initialize pub/sub channel.
                self.shard.init_pub_sub();
            }
            self.primary_id = primary_id;
            changed
        } else {
            false
        }
    }

    /// Role detector is enabled.
    pub(super) fn enabled(&self) -> bool {
        self.enabled
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
    use crate::config::{ReadWriteSplit, Role};
    use pgdog_stats::LsnStats as StatsLsnStats;

    use super::super::ShardConfig;
    use super::*;

    fn create_test_pool_config(host: &str, port: u16, role_detection: bool) -> PoolConfig {
        PoolConfig {
            address: Address {
                host: host.into(),
                port,
                user: "pgdog".into(),
                passwords: vec!["pgdog".into()],
                database_name: "pgdog".into(),
                configured_role: if role_detection {
                    Role::Auto
                } else {
                    Role::Replica
                },
                ..Default::default()
            },
            config: Config {
                inner: pgdog_stats::Config {
                    role_detection,
                    ..Config::default().inner
                },
            },
        }
    }

    fn create_test_shard(primary: Option<&PoolConfig>, replicas: &[PoolConfig]) -> Shard {
        Shard::new(ShardConfig {
            primary,
            replicas,
            rw_split: ReadWriteSplit::ExcludePrimary,
            identifier: Arc::new(User {
                user: "pgdog".into(),
                database: "pgdog".into(),
            }),
            lsn_check_interval: Duration::MAX,
            ..Default::default()
        })
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

    #[test]
    fn test_changed_revokes_primary_when_lsn_stats_invalid() {
        let primary = Some(create_test_pool_config("127.0.0.1", 5432, true));
        let replicas = [create_test_pool_config("localhost", 5432, true)];
        let shard = create_test_shard(primary.as_ref(), &replicas);

        let mut detector = RoleDetector::new(&shard);

        assert!(detector.enabled());
        assert!(detector.changed());
        assert!(!detector.changed());
    }

    #[test]
    fn test_changed_returns_false_when_roles_unchanged() {
        let primary = Some(create_test_pool_config("127.0.0.1", 5432, true));
        let replicas = [create_test_pool_config("localhost", 5432, true)];
        let shard = create_test_shard(primary.as_ref(), &replicas);

        set_lsn_stats(&shard, 0, true, 100);
        set_lsn_stats(&shard, 1, false, 200);

        let mut detector = RoleDetector::new(&shard);

        assert!(detector.enabled());
        assert!(!detector.changed());
    }

    #[test]
    fn test_changed_returns_true_after_external_failover_detection() {
        let primary = Some(create_test_pool_config("127.0.0.1", 5432, true));
        let replicas = [create_test_pool_config("localhost", 5432, true)];
        let shard = create_test_shard(primary.as_ref(), &replicas);

        set_lsn_stats(&shard, 0, true, 100);
        set_lsn_stats(&shard, 1, false, 200);

        let mut detector = RoleDetector::new(&shard);

        assert!(detector.enabled());
        assert!(!detector.changed());

        set_lsn_stats(&shard, 0, false, 300);
        set_lsn_stats(&shard, 1, true, 200);

        assert!(shard.redetect_roles());
        assert!(detector.changed());
    }

    #[test]
    fn test_changed_returns_false_after_roles_stabilize() {
        let primary = Some(create_test_pool_config("127.0.0.1", 5432, true));
        let replicas = [create_test_pool_config("localhost", 5432, true)];
        let shard = create_test_shard(primary.as_ref(), &replicas);

        set_lsn_stats(&shard, 0, true, 100);
        set_lsn_stats(&shard, 1, false, 200);

        let mut detector = RoleDetector::new(&shard);
        assert!(detector.enabled());
        assert!(!detector.changed());

        set_lsn_stats(&shard, 0, false, 300);
        set_lsn_stats(&shard, 1, true, 200);

        assert!(detector.changed());

        assert!(!detector.changed());
    }

    #[test]
    fn test_disabled_when_not_all_roles_auto() {
        let primary = Some(create_test_pool_config("127.0.0.1", 5432, false));
        let replicas = [create_test_pool_config("localhost", 5432, true)];
        let shard = create_test_shard(primary.as_ref(), &replicas);

        set_lsn_stats(&shard, 0, true, 100);
        set_lsn_stats(&shard, 1, false, 200);

        let mut detector = RoleDetector::new(&shard);

        assert!(!detector.enabled());
        assert!(!detector.changed());

        set_lsn_stats(&shard, 0, false, 300);
        set_lsn_stats(&shard, 1, true, 200);

        assert!(!detector.changed());
    }
}
