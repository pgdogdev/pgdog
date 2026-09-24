use pgdog_config::Role;

use crate::backend::Shard;
use crate::backend::pool::Address;

pub(crate) fn resolve_resharding_replicas(shard: &Shard) -> Vec<Address> {
    let resharding_only = shard
        .pools()
        .into_iter()
        .filter(|pool| pool.config().resharding_only)
        .map(|pool| pool.addr().clone())
        .collect::<Vec<_>>();

    if resharding_only.is_empty() {
        let include_primary = !shard.has_replicas();

        shard
            .pools_with_roles()
            .into_iter()
            .filter(|(r, _)| match *r {
                Role::Replica => true,
                Role::Primary => include_primary,
                Role::Auto => false,
            })
            .map(|(_, p)| p.addr().clone())
            .collect::<Vec<_>>()
    } else {
        resharding_only
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::pool::{Cluster, ClusterConfig, ClusterShardConfig, PoolConfig};

    fn pool(port: u16, role: Role, resharding_only: bool) -> PoolConfig {
        let mut pool = PoolConfig {
            address: Address {
                port,
                configured_role: role,
                ..Address::new_test()
            },
            ..Default::default()
        };
        pool.config.resharding_only = resharding_only;
        pool
    }

    fn assert_resolves(primary: PoolConfig, replicas: Vec<PoolConfig>, expected: &[u16]) {
        let cluster = Cluster::new(ClusterConfig::new(
            &Default::default(),
            &Default::default(),
            &[ClusterShardConfig {
                primary: Some(primary),
                replicas,
            }],
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        ));
        let mut ports: Vec<_> = resolve_resharding_replicas(&cluster.shards()[0])
            .into_iter()
            .map(|address| address.port)
            .collect();
        ports.sort_unstable();
        assert_eq!(ports, expected);
    }

    #[test]
    fn prefers_resharding_only_replica() {
        assert_resolves(
            pool(5432, Role::Primary, false),
            vec![
                pool(5433, Role::Replica, false),
                pool(5434, Role::Replica, true),
            ],
            &[5434],
        );
    }

    #[test]
    fn excludes_primary_when_replicas_exist() {
        assert_resolves(
            pool(5432, Role::Primary, false),
            vec![
                pool(5433, Role::Replica, false),
                pool(5434, Role::Replica, false),
            ],
            &[5433, 5434],
        );
    }

    #[test]
    fn falls_back_to_primary() {
        assert_resolves(pool(5432, Role::Primary, false), vec![], &[5432]);
    }

    #[test]
    fn prefers_resharding_only_primary_over_regular_replicas() {
        assert_resolves(
            pool(5432, Role::Primary, true),
            vec![pool(5433, Role::Replica, false)],
            &[5432],
        );
    }

    #[test]
    fn includes_all_resharding_only_pools_and_excludes_regular_pools() {
        assert_resolves(
            pool(5432, Role::Primary, true),
            vec![
                pool(5433, Role::Replica, true),
                pool(5434, Role::Replica, false),
                pool(5435, Role::Replica, true),
            ],
            &[5432, 5433, 5435],
        );
    }
}
