use crate::config::LoadBalancingStrategy;

// use super::pool;
use super::*;
use tokio::spawn;

fn replicas() -> Replicas {
    let one = PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "pgdog".into(),
            password: "pgdog".into(),
            database_name: "pgdog".into(),
            ..Default::default()
        },
        config: Config {
            max: 1,
            checkout_timeout: Duration::from_millis(1000),
            ..Default::default()
        },
    };
    let mut two = one.clone();
    two.address.host = "localhost".into();
    let replicas = Replicas::new(&None, &[one, two], LoadBalancingStrategy::Random);
    replicas.pools().iter().for_each(|p| p.launch());
    replicas
}

#[tokio::test]
async fn test_replicas() {
    let replicas = replicas();

    for pool in 0..2 {
        let mut tasks = vec![];
        replicas.replica_pools[pool].ban(Error::CheckoutTimeout);

        for _ in 0..10000 {
            let replicas = replicas.clone();
            let other = if pool == 0 { 1 } else { 0 };
            tasks.push(spawn(async move {
                assert!(replicas.replica_pools[pool].server_error());
                assert!(!replicas.replica_pools[other].server_error());
                let conn = replicas.get(&Request::default(), false).await.unwrap();
                assert_eq!(conn.addr(), replicas.replica_pools[other].addr());
                assert!(replicas.replica_pools[pool].server_error());
                assert!(!replicas.replica_pools[other].server_error());
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        replicas.replica_pools[pool].maybe_unban();
    }

    replicas.replica_pools[0].ban(Error::CheckoutTimeout);
    replicas.replica_pools[1].ban(Error::CheckoutTimeout);

    // All replicas banned, unban everyone.
    assert!(replicas
        .replica_pools
        .iter()
        .all(|pool| pool.server_error()));
    replicas.get(&Request::default(), false).await.unwrap();
    assert!(replicas
        .replica_pools
        .iter()
        .all(|pool| !pool.server_error()));
}
