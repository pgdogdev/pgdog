use super::Shard;

pub(super) struct RoleDetector {
    enabled: bool,
    shard: Shard,
}

impl RoleDetector {
    /// Create new role change detector.
    pub(super) fn new(shard: &Shard) -> Self {
        Self {
            enabled: shard
                .pools()
                .iter()
                .all(|pool| pool.config().role_detection),
            shard: shard.clone(),
        }
    }

    /// Detect role change in the shard.
    pub(super) fn changed(&mut self) -> bool {
        if self.enabled() {
            self.shard.redetect_roles()
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
    use std::time::Duration;

    use tokio::time::Instant;

    use crate::backend::databases::User;
    use crate::backend::pool::lsn_monitor::LsnStats;
    use crate::backend::pool::{Address, Config, PoolConfig};
    use crate::backend::replication::publisher::Lsn;
    use crate::config::{LoadBalancingStrategy, ReadWriteSplit};

    use super::super::ShardConfig;
    use super::*;

    fn create_test_pool_config(host: &str, port: u16, role_detection: bool) -> PoolConfig {
        PoolConfig {
            address: Address {
                host: host.into(),
                port,
                user: "pgdog".into(),
                password: "pgdog".into(),
                database_name: "pgdog".into(),
                ..Default::default()
            },
            config: Config {
                role_detection,
                ..Default::default()
            },
        }
    }

    fn create_test_shard(primary: &Option<PoolConfig>, replicas: &[PoolConfig]) -> Shard {
        Shard::new(ShardConfig {
            number: 0,
            primary,
            replicas,
            lb_strategy: LoadBalancingStrategy::Random,
            rw_split: ReadWriteSplit::ExcludePrimary,
            identifier: Arc::new(User {
                user: "pgdog".into(),
                database: "pgdog".into(),
            }),
            lsn_check_interval: Duration::MAX,
        })
    }

    fn set_lsn_stats(shard: &Shard, index: usize, replica: bool, lsn: i64) {
        let pools = shard.pools();
        let stats = LsnStats {
            replica,
            lsn: Lsn::from_i64(lsn),
            offset_bytes: lsn,
            fetched: Instant::now(),
            ..Default::default()
        };
        *pools[index].inner().lsn_stats.write() = stats;
    }

    #[test]
    fn test_changed_returns_false_when_lsn_stats_invalid() {
        let primary = Some(create_test_pool_config("127.0.0.1", 5432, true));
        let replicas = [create_test_pool_config("localhost", 5432, true)];
        let shard = create_test_shard(&primary, &replicas);

        let mut detector = RoleDetector::new(&shard);

        assert!(detector.enabled());
        assert!(!detector.changed());
    }

    #[test]
    fn test_changed_returns_false_when_roles_unchanged() {
        let primary = Some(create_test_pool_config("127.0.0.1", 5432, true));
        let replicas = [create_test_pool_config("localhost", 5432, true)];
        let shard = create_test_shard(&primary, &replicas);

        set_lsn_stats(&shard, 0, true, 100);
        set_lsn_stats(&shard, 1, false, 200);

        let mut detector = RoleDetector::new(&shard);

        assert!(detector.enabled());
        assert!(!detector.changed());
    }

    #[test]
    fn test_changed_returns_true_on_failover() {
        let primary = Some(create_test_pool_config("127.0.0.1", 5432, true));
        let replicas = [create_test_pool_config("localhost", 5432, true)];
        let shard = create_test_shard(&primary, &replicas);

        set_lsn_stats(&shard, 0, true, 100);
        set_lsn_stats(&shard, 1, false, 200);

        let mut detector = RoleDetector::new(&shard);

        assert!(detector.enabled());
        assert!(!detector.changed());

        set_lsn_stats(&shard, 0, false, 300);
        set_lsn_stats(&shard, 1, true, 200);

        assert!(detector.changed());
    }

    #[test]
    fn test_changed_returns_false_after_roles_stabilize() {
        let primary = Some(create_test_pool_config("127.0.0.1", 5432, true));
        let replicas = [create_test_pool_config("localhost", 5432, true)];
        let shard = create_test_shard(&primary, &replicas);

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
        let shard = create_test_shard(&primary, &replicas);

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
