use std::collections::HashSet;
use std::time::Duration;
use tokio::time::sleep;

use crate::backend::pool::{Address, Config, Error, PoolConfig, Request};
use crate::config::LoadBalancingStrategy;

use super::*;
use monitor::Monitor;

fn create_test_pool_config(host: &str, port: u16) -> PoolConfig {
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
            max: 1,
            checkout_timeout: Duration::from_millis(1000),
            ban_timeout: Duration::from_millis(100),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn setup_test_replicas() -> LoadBalancer {
    let pool_config1 = create_test_pool_config("127.0.0.1", 5432);
    let pool_config2 = create_test_pool_config("localhost", 5432);

    let replicas = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();
    replicas
}

#[tokio::test]
async fn test_replica_ban_recovery_after_timeout() {
    let replicas = setup_test_replicas();

    // Ban the first replica with very short timeout
    let ban = &replicas.targets[0].ban;
    ban.ban(Error::ServerError, Duration::from_millis(50));

    assert!(ban.banned());

    // Wait for ban to expire
    sleep(Duration::from_millis(60)).await;

    // Check if ban would be removed (simulate monitor behavior)
    let now = std::time::Instant::now();
    let unbanned = ban.unban_if_expired(now);

    assert!(unbanned);
    assert!(!ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_replica_manual_unban() {
    let replicas = setup_test_replicas();

    // Ban the first replica
    let ban = &replicas.targets[0].ban;
    ban.ban(Error::ServerError, Duration::from_millis(1000));

    assert!(ban.banned());

    // Manually unban
    ban.unban(false);

    assert!(!ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_replica_ban_error_retrieval() {
    let replicas = setup_test_replicas();

    let ban = &replicas.targets[0].ban;

    // No error initially
    assert!(ban.error().is_none());

    // Ban with specific error
    ban.ban(Error::ServerError, Duration::from_millis(100));

    // Should return the ban error
    let error = ban.error().unwrap();
    assert!(matches!(error, Error::ServerError));

    replicas.shutdown();
}

#[tokio::test]
async fn test_multiple_replica_banning() {
    let replicas = setup_test_replicas();

    // Ban both replicas
    for i in 0..2 {
        let ban = &replicas.targets[i].ban;
        ban.ban(Error::ServerError, Duration::from_millis(100));

        assert!(ban.banned());
    }

    // Both should be banned
    assert_eq!(
        replicas.targets.iter().filter(|r| r.ban.banned()).count(),
        2
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_replica_ban_idempotency() {
    let replicas = setup_test_replicas();

    let ban = &replicas.targets[0].ban;

    // First ban should succeed
    let first_ban = ban.ban(Error::ServerError, Duration::from_millis(100));
    assert!(first_ban);
    assert!(ban.banned());

    // Second ban of same replica should not create new ban
    let second_ban = ban.ban(Error::ConnectTimeout, Duration::from_millis(200));
    assert!(!second_ban);
    assert!(ban.banned());

    // Error should still be the original one
    assert!(matches!(ban.error().unwrap(), Error::ServerError));

    replicas.shutdown();
}

#[tokio::test]
async fn test_pools_with_roles_and_bans() {
    let replicas = setup_test_replicas();

    let pools_info = replicas.pools_with_roles_and_bans();

    // Should have 2 replica pools (no primary in this test)
    assert_eq!(pools_info.len(), 2);

    // All should be replica role
    for (role, _ban, _pool) in &pools_info {
        assert!(matches!(role, crate::config::Role::Replica));
    }

    replicas.shutdown();
}

#[tokio::test]
async fn test_primary_pool_banning() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config("localhost", 5432)];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    // Test primary ban exists
    assert!(replicas.primary_target().is_some());

    let primary_ban = &replicas.primary_target().unwrap().ban;

    // Ban primary for reads
    primary_ban.ban(Error::ServerError, Duration::from_millis(100));

    assert!(primary_ban.banned());

    // Check pools with roles includes primary
    let pools_info = replicas.pools_with_roles_and_bans();
    assert_eq!(pools_info.len(), 2); // 1 replica + 1 primary

    let has_primary = pools_info
        .iter()
        .any(|(role, _ban, _pool)| matches!(role, crate::config::Role::Primary));
    assert!(has_primary);

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_ban_timeout_not_expired() {
    let replicas = setup_test_replicas();

    let ban = &replicas.targets[0].ban;
    ban.ban(Error::ServerError, Duration::from_millis(1000)); // Long timeout

    assert!(ban.banned());

    // Check immediately - should not be expired
    let now = std::time::Instant::now();
    let unbanned = ban.unban_if_expired(now);

    assert!(!unbanned);
    assert!(ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_unban_if_expired_checks_pool_health() {
    let replicas = setup_test_replicas();

    let ban = &replicas.targets[0].ban;
    let pool = &replicas.targets[0].pool;

    ban.ban(Error::ServerError, Duration::from_millis(50));
    assert!(ban.banned());

    pool.inner().health.toggle(false);

    sleep(Duration::from_millis(60)).await;

    let now = std::time::Instant::now();
    let unbanned = ban.unban_if_expired(now);

    assert!(!unbanned);
    assert!(ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_replica_ban_clears_idle_connections() {
    let replicas = setup_test_replicas();

    // Get a connection and return it to create idle connections
    let request = Request::default();
    let conn = replicas.pools()[0]
        .get(&request)
        .await
        .expect("Should be able to get connection from launched pool");

    // Verify we have a valid connection
    assert!(!conn.error());

    drop(conn); // Return to pool as idle

    // Give a moment for the connection to be properly returned to idle state
    sleep(Duration::from_millis(10)).await;

    // Check that we have idle connections before banning
    let idle_before = replicas.pools()[0].lock().idle();
    assert!(
        idle_before > 0,
        "Should have idle connections before banning, but found {}",
        idle_before
    );

    let ban = &replicas.targets[0].ban;

    // Ban should trigger dump_idle() on the pool
    ban.ban(Error::ServerError, Duration::from_millis(100));

    // Verify the ban was applied
    assert!(ban.banned());

    // Verify that idle connections were cleared
    let idle_after = replicas.pools()[0].lock().idle();
    assert_eq!(
        idle_after, 0,
        "Idle connections should be cleared after banning"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_monitor_automatic_ban_expiration() {
    let replicas = setup_test_replicas();

    // Ban the first replica with very short timeout
    let ban = &replicas.targets[0].ban;
    ban.ban(Error::ServerError, Duration::from_millis(100));

    assert!(ban.banned());

    // Wait longer than the ban timeout to allow monitor to process
    // The monitor runs every 333ms, so we wait for at least one cycle
    sleep(Duration::from_millis(400)).await;

    // The monitor should have automatically unbanned the replica
    // Note: Since the monitor runs in a background task spawned during Replicas::new(),
    // and we can't easily control its timing in tests, we check that the ban
    // can be expired when checked
    let now = std::time::Instant::now();
    let would_be_unbanned = ban.unban_if_expired(now);

    // Either it was already unbanned by the monitor, or it would be unbanned now
    assert!(!ban.banned() || would_be_unbanned);

    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_exclude_primary() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [
        create_test_pool_config("localhost", 5432),
        create_test_pool_config("127.0.0.1", 5432),
    ];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::ExcludePrimary,
    );
    replicas.launch();

    let request = Request::default();

    // Try getting connections multiple times and verify primary is never used
    let mut replica_ids = HashSet::new();
    for _ in 0..100 {
        let conn = replicas.get(&request).await.unwrap();
        replica_ids.insert(conn.pool.id());
    }

    // Should only use replica pools, not primary
    assert_eq!(replica_ids.len(), 2);

    // Verify primary pool ID is not in the set of used pools
    let primary_id = replicas.primary().unwrap().id();
    assert!(!replica_ids.contains(&primary_id));

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_include_primary() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config("localhost", 5432)];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    let request = Request::default();

    // Try getting connections multiple times and verify both primary and replica can be used
    let mut used_pool_ids = HashSet::new();
    for _ in 0..20 {
        let conn = replicas.get(&request).await.unwrap();
        used_pool_ids.insert(conn.pool.id());
    }

    // Should use both primary and replica pools
    assert_eq!(used_pool_ids.len(), 2);

    // Verify primary pool ID is in the set of used pools
    let primary_id = replicas.primary().unwrap().id();
    assert!(used_pool_ids.contains(&primary_id));

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_exclude_primary_no_primary() {
    // Test exclude primary setting when no primary exists
    let replica_configs = [
        create_test_pool_config("localhost", 5432),
        create_test_pool_config("127.0.0.1", 5432),
    ];

    let replicas = LoadBalancer::new(
        &None,
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::ExcludePrimary,
    );
    replicas.launch();

    let request = Request::default();

    // Should work normally with just replicas
    let mut replica_ids = HashSet::new();
    for _ in 0..10 {
        let conn = replicas.get(&request).await.unwrap();
        replica_ids.insert(conn.pool.id());
    }

    assert_eq!(replica_ids.len(), 2);

    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_include_primary_no_primary() {
    // Test include primary setting when no primary exists
    let replica_configs = [
        create_test_pool_config("localhost", 5432),
        create_test_pool_config("127.0.0.1", 5432),
    ];

    let replicas = LoadBalancer::new(
        &None,
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    let request = Request::default();

    // Should work normally with just replicas
    let mut replica_ids = HashSet::new();
    for _ in 0..10 {
        let conn = replicas.get(&request).await.unwrap();
        replica_ids.insert(conn.pool.id());
    }

    // Should use both replica pools
    assert_eq!(replica_ids.len(), 2);

    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_with_banned_primary() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config("localhost", 5432)];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    // Ban the primary
    let primary_ban = &replicas.targets.last().unwrap().ban;
    primary_ban.ban(Error::ServerError, Duration::from_millis(1000));

    let request = Request::default();

    // Should only use replica even though primary inclusion is enabled
    let mut used_pool_ids = HashSet::new();
    for _ in 0..10 {
        let conn = replicas.get(&request).await.unwrap();
        used_pool_ids.insert(conn.pool.id());
    }

    // Should only use replica pool since primary is banned
    assert_eq!(used_pool_ids.len(), 1);

    // Verify primary pool ID is not in the set of used pools
    let primary_id = replicas.targets.last().unwrap().pool.id();
    assert!(!used_pool_ids.contains(&primary_id));

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_with_banned_replicas() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config("localhost", 5432)];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    // Ban the replica
    let replica_ban = &replicas.targets[0].ban;
    replica_ban.ban(Error::ServerError, Duration::from_millis(1000));

    let request = Request::default();

    // Should only use primary since replica is banned
    let mut used_pool_ids = HashSet::new();
    for _ in 0..10 {
        let conn = replicas.get(&request).await.unwrap();
        used_pool_ids.insert(conn.pool.id());
    }

    // Should only use primary pool since replica is banned
    assert_eq!(used_pool_ids.len(), 1);

    // Verify primary pool ID is in the set of used pools
    let primary_id = replicas.targets.last().unwrap().pool.id();
    assert!(used_pool_ids.contains(&primary_id));

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_exclude_primary_with_round_robin() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [
        create_test_pool_config("localhost", 5432),
        create_test_pool_config("127.0.0.1", 5432),
    ];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::ExcludePrimary,
    );
    replicas.launch();

    let request = Request::default();

    // Collect pool IDs from multiple requests to verify round-robin behavior
    let mut pool_sequence = Vec::new();
    for _ in 0..8 {
        let conn = replicas.get(&request).await.unwrap();
        pool_sequence.push(conn.pool.id());
    }

    // Should use both replicas (round-robin)
    let unique_ids: HashSet<_> = pool_sequence.iter().collect();
    assert_eq!(unique_ids.len(), 2);

    // Verify primary is never used
    let primary_id = replicas.targets.last().unwrap().pool.id();
    assert!(!pool_sequence.contains(&primary_id));

    // Verify round-robin pattern: each pool should be different from the previous one
    for i in 1..pool_sequence.len() {
        assert_ne!(
            pool_sequence[i],
            pool_sequence[i - 1],
            "Round-robin pattern broken: consecutive pools are the same at positions {} and {}",
            i - 1,
            i
        );
    }

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_monitor_shuts_down_on_notify() {
    let pool_config1 = create_test_pool_config("127.0.0.1", 5432);
    let pool_config2 = create_test_pool_config("localhost", 5432);

    let replicas = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    replicas
        .targets
        .iter()
        .for_each(|target| target.pool.launch());
    let monitor_handle = Monitor::spawn(&replicas);

    // Give monitor time to start and register notified() future
    sleep(Duration::from_millis(10)).await;

    replicas.shutdown();

    let result = tokio::time::timeout(Duration::from_secs(1), monitor_handle).await;

    assert!(
        result.is_ok(),
        "Monitor should shut down within timeout after notify"
    );
    assert!(
        result.unwrap().is_ok(),
        "Monitor task should complete successfully"
    );
}

#[tokio::test]
async fn test_monitor_bans_unhealthy_target() {
    let replicas = setup_test_replicas();

    replicas.targets[0].health.toggle(false);

    sleep(Duration::from_millis(400)).await;

    assert!(replicas.targets[0].ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_monitor_clears_expired_bans() {
    let replicas = setup_test_replicas();

    replicas.targets[0]
        .ban
        .ban(Error::ServerError, Duration::from_millis(50));

    sleep(Duration::from_millis(400)).await;

    assert!(!replicas.targets[0].ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_monitor_does_not_ban_single_target() {
    let pool_config = create_test_pool_config("127.0.0.1", 5432);

    let replicas = LoadBalancer::new(
        &None,
        &[pool_config],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    replicas.targets[0].health.toggle(false);

    sleep(Duration::from_millis(400)).await;

    assert!(!replicas.targets[0].ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_monitor_unbans_all_when_all_unhealthy() {
    let replicas = setup_test_replicas();

    replicas.targets[0].health.toggle(false);
    replicas.targets[1].health.toggle(false);

    sleep(Duration::from_millis(400)).await;

    assert!(!replicas.targets[0].ban.banned());
    assert!(!replicas.targets[1].ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_monitor_does_not_ban_with_zero_ban_timeout() {
    let pool_config1 = PoolConfig {
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
            ban_timeout: Duration::ZERO,
            ..Default::default()
        },
        ..Default::default()
    };

    let pool_config2 = PoolConfig {
        address: Address {
            host: "localhost".into(),
            port: 5432,
            user: "pgdog".into(),
            password: "pgdog".into(),
            database_name: "pgdog".into(),
            ..Default::default()
        },
        config: Config {
            max: 1,
            checkout_timeout: Duration::from_millis(1000),
            ban_timeout: Duration::ZERO,
            ..Default::default()
        },
        ..Default::default()
    };

    let replicas = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    replicas.targets[0].health.toggle(false);

    sleep(Duration::from_millis(400)).await;

    assert!(!replicas.targets[0].ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_monitor_health_state_race() {
    use tokio::spawn;

    let replicas = setup_test_replicas();
    let target = replicas.targets[0].clone();

    let toggle_task = spawn(async move {
        for _ in 0..50 {
            target.health.toggle(false);
            sleep(Duration::from_micros(100)).await;
            target.health.toggle(true);
            sleep(Duration::from_micros(100)).await;
        }
    });

    sleep(Duration::from_millis(500)).await;

    toggle_task.await.unwrap();

    let banned = replicas.targets[0].ban.banned();
    let healthy = replicas.targets[0].health.healthy();

    assert!(
        !banned || !healthy,
        "Pool should not be banned if healthy after race"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_include_primary_if_replica_banned_no_bans() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config("localhost", 5432)];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    );
    replicas.launch();

    let request = Request::default();

    // When no replicas are banned, primary should NOT be used
    let mut used_pool_ids = HashSet::new();
    for _ in 0..20 {
        let conn = replicas.get(&request).await.unwrap();
        used_pool_ids.insert(conn.pool.id());
    }

    // Should only use replica pool
    assert_eq!(used_pool_ids.len(), 1);

    // Verify primary pool ID is not in the set of used pools
    let primary_id = replicas.primary().unwrap().id();
    assert!(!used_pool_ids.contains(&primary_id));

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_include_primary_if_replica_banned_with_ban() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config("localhost", 5432)];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    );
    replicas.launch();

    // Ban the replica
    let replica_ban = &replicas.targets[0].ban;
    replica_ban.ban(Error::ServerError, Duration::from_millis(1000));

    let request = Request::default();

    // When replica is banned, primary SHOULD be used
    let mut used_pool_ids = HashSet::new();
    for _ in 0..20 {
        let conn = replicas.get(&request).await.unwrap();
        used_pool_ids.insert(conn.pool.id());
    }

    // Should only use primary pool since replica is banned
    assert_eq!(used_pool_ids.len(), 1);

    // Verify primary pool ID is in the set of used pools
    let primary_id = replicas.primary().unwrap().id();
    assert!(used_pool_ids.contains(&primary_id));

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_has_replicas_with_replicas() {
    let replicas = setup_test_replicas();

    assert!(replicas.has_replicas());

    replicas.shutdown();
}

#[tokio::test]
async fn test_has_replicas_with_primary_and_replicas() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config("localhost", 5432)];

    let lb = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    lb.launch();

    assert!(lb.has_replicas());

    lb.shutdown();
}

#[tokio::test]
async fn test_has_replicas_primary_only() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let lb = LoadBalancer::new(
        &Some(primary_pool),
        &[],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    lb.launch();

    assert!(!lb.has_replicas());

    lb.shutdown();
}

#[tokio::test]
async fn test_has_replicas_empty() {
    let lb = LoadBalancer::new(
        &None,
        &[],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    assert!(!lb.has_replicas());
}

#[tokio::test]
async fn test_set_role() {
    let replicas = setup_test_replicas();

    // Initially all targets are replicas
    assert_eq!(replicas.targets[0].role(), Role::Replica);
    assert_eq!(replicas.targets[1].role(), Role::Replica);

    // Setting replica to replica returns false (no change)
    let changed = replicas.targets[0].set_role(Role::Replica);
    assert!(!changed);
    assert_eq!(replicas.targets[0].role(), Role::Replica);

    // Setting replica to primary returns true (changed)
    let changed = replicas.targets[0].set_role(Role::Primary);
    assert!(changed);
    assert_eq!(replicas.targets[0].role(), Role::Primary);

    // Setting primary to primary returns false (no change)
    let changed = replicas.targets[0].set_role(Role::Primary);
    assert!(!changed);
    assert_eq!(replicas.targets[0].role(), Role::Primary);

    // Setting primary to replica returns true (changed)
    let changed = replicas.targets[0].set_role(Role::Replica);
    assert!(changed);
    assert_eq!(replicas.targets[0].role(), Role::Replica);

    replicas.shutdown();
}

#[tokio::test]
async fn test_can_move_conns_to_same_config() {
    let pool_config1 = create_test_pool_config("127.0.0.1", 5432);
    let pool_config2 = create_test_pool_config("localhost", 5432);

    let lb1 = LoadBalancer::new(
        &None,
        &[pool_config1.clone(), pool_config2.clone()],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    let lb2 = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    assert!(lb1.can_move_conns_to(&lb2));
}

#[tokio::test]
async fn test_can_move_conns_to_different_count() {
    let pool_config1 = create_test_pool_config("127.0.0.1", 5432);
    let pool_config2 = create_test_pool_config("localhost", 5432);

    let lb1 = LoadBalancer::new(
        &None,
        &[pool_config1.clone(), pool_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    let lb2 = LoadBalancer::new(
        &None,
        &[pool_config1],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    assert!(!lb1.can_move_conns_to(&lb2));
}

#[tokio::test]
async fn test_can_move_conns_to_different_addresses() {
    let pool_config1 = create_test_pool_config("127.0.0.1", 5432);
    let pool_config2 = create_test_pool_config("localhost", 5432);
    let pool_config3 = create_test_pool_config("127.0.0.1", 5433);

    let lb1 = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    let lb2 = LoadBalancer::new(
        &None,
        &[pool_config3.clone(), pool_config3],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    assert!(!lb1.can_move_conns_to(&lb2));
}

#[tokio::test]
async fn test_monitor_unbans_all_when_second_target_becomes_unhealthy_after_first_banned() {
    let replicas = setup_test_replicas();

    // First target becomes unhealthy
    replicas.targets[0].health.toggle(false);

    // Wait for monitor to ban the first target
    sleep(Duration::from_millis(400)).await;

    assert!(
        replicas.targets[0].ban.banned(),
        "First target should be banned"
    );
    assert!(
        !replicas.targets[1].ban.banned(),
        "Second target should not be banned yet"
    );

    // Now second target becomes unhealthy (first is already banned)
    replicas.targets[1].health.toggle(false);

    // Wait for monitor to process - should unban all since all are unhealthy
    sleep(Duration::from_millis(400)).await;

    // Both should be unbanned because all targets are unhealthy
    assert!(
        !replicas.targets[0].ban.banned(),
        "First target should be unbanned when all targets are unhealthy"
    );
    assert!(
        !replicas.targets[1].ban.banned(),
        "Second target should be unbanned when all targets are unhealthy"
    );

    replicas.shutdown();
}
