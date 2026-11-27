use super::*;

#[tokio::test]
async fn test_least_active_connections_prefers_pool_with_fewer_checked_out() {
    let replicas = test_lb()
        .configs(&replica_pool_configs(2))
        .strategy(LoadBalancingStrategy::LeastActiveConnections)
        .launch();

    let request = LbRequest::default();

    // Get first connection and hold it
    let conn1 = replicas.get(&request).await.unwrap();
    let first_pool_id = conn1.pool.id();

    // Now first pool has 1 checked out, second pool has 0.
    // LeastActiveConnections should select the pool with 0 checked out.
    let conn2 = replicas.get(&request).await.unwrap();
    let second_pool_id = conn2.pool.id();

    // conn2 should come from a different pool (the one with 0 checked out)
    assert_ne!(
        first_pool_id, second_pool_id,
        "LeastActiveConnections should select the pool with fewer checked-out connections"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_least_active_connections_returns_to_pool_after_checkin() {
    let replicas = test_lb()
        .configs(&replica_pool_configs(2))
        .strategy(LoadBalancingStrategy::LeastActiveConnections)
        .launch();

    let request = LbRequest::default();

    let conn1 = replicas.get(&request).await.unwrap();
    let conn2 = replicas.get(&request).await.unwrap();
    let freed_pool_id = conn1.pool.id();
    assert_ne!(freed_pool_id, conn2.pool.id());

    drop(conn1);
    assert_eq!(replicas.targets[0].pool.lock().checked_out(), 0);

    let conn3 = replicas.get(&request).await.unwrap();
    assert_eq!(
        conn3.pool.id(),
        freed_pool_id,
        "check-in must lower the count so the freed pool is preferred again"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_least_active_connections_avoids_busier_pool() {
    let replicas = test_lb()
        .configs(&replica_pool_configs(2))
        .strategy(LoadBalancingStrategy::LeastActiveConnections)
        .launch();

    let busy = replicas.targets[0]
        .pool
        .get(&crate::backend::pool::Request::default())
        .await
        .unwrap();

    let request = LbRequest::default();
    let conn = replicas.get(&request).await.unwrap();
    assert_eq!(
        conn.pool.id(),
        replicas.targets[1].pool.id(),
        "the pool with fewer checked-out connections must be selected"
    );

    drop(conn);
    drop(busy);
    replicas.shutdown();
}

#[tokio::test]
async fn test_least_active_connections_not_skewed_by_dropped_waiter() {
    use crate::backend::pool::{Request, waiting::Waiting};
    use crate::net::messages::FrontendPid;

    let replicas = test_lb()
        .configs(&replica_pool_configs(2))
        .strategy(LoadBalancingStrategy::LeastActiveConnections)
        .launch();

    let pool = &replicas.targets[0].pool;
    drop(pool.get(&Request::default()).await.unwrap());
    assert!(pool.lock().idle() >= 1);

    let waiting = Waiting::new(pool.clone(), &Request::unrouted(FrontendPid::new())).unwrap();
    drop(waiting);

    assert_eq!(
        pool.lock().checked_out(),
        0,
        "a server handed to a dropped waiter must not count as checked out"
    );

    let shares = checkout_shares(&replicas, 10).await;
    assert!(
        shares[0] > 0,
        "the first replica must still get traffic, got {shares:?}"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_least_active_connections_not_skewed_by_polled_dropped_waiter() {
    use crate::backend::pool::{Request, waiting::Waiting};
    use crate::net::messages::FrontendPid;

    let mut configs = replica_pool_configs(2);
    for config in &mut configs {
        config.config.max = 1;
    }
    let replicas = test_lb()
        .configs(&configs)
        .strategy(LoadBalancingStrategy::LeastActiveConnections)
        .launch();

    let pool = &replicas.targets[0].pool;
    let held = pool.get(&Request::default()).await.unwrap();

    let mut waiting = Waiting::new(pool.clone(), &Request::unrouted(FrontendPid::new())).unwrap();
    {
        let mut wait = Box::pin(waiting.wait());
        assert!(futures::poll!(wait.as_mut()).is_pending());

        drop(held);
        assert_eq!(pool.lock().checked_out(), 1);
    }
    drop(waiting);

    let guard = pool.lock();
    assert!(guard.waiting.is_empty());
    assert_eq!(
        guard.checked_out(),
        0,
        "a server handed to a polled, dropped waiter must not count as checked out"
    );
    assert_eq!(guard.idle(), 1);
    drop(guard);

    replicas.shutdown();
}
