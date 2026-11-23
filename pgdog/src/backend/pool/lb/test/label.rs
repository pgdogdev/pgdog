use super::*;

fn labeled_replicas() -> LoadBalancer {
    let mut reporting = create_test_pool_config("127.0.0.1", 5432);
    reporting.address.labels = vec!["reporting".into()];
    let mut analytics = create_test_pool_config("localhost", 5432);
    analytics.address.labels = vec!["reporting".into(), "analytics".into()];
    let mut backup = create_test_pool_config("127.0.0.1", 5432);
    backup.address.labels = vec!["backup".into()];

    let replicas = LoadBalancer::new(
        &None,
        &[reporting, analytics, backup],
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::ExcludePrimary,
        Default::default(),
    );
    // Keep bans under test control instead of running the monitor that clears them.
    for target in &replicas.targets {
        target.pool.launch();
    }
    replicas
}

async fn get_with_label(replicas: &LoadBalancer, label: Option<&str>) -> Result<Guard, Error> {
    let mut route = Route::read(ShardWithPriority::default());
    route.label = label.map(str::to_owned);
    let request = LbRequest::new(Default::default(), &route, false, Instant::now());
    replicas.get(&request).await
}

#[tokio::test]
async fn test_label_required() {
    let reporting = create_test_pool_config("127.0.0.1", 5432);
    let mut analytics = create_test_pool_config("localhost", 5432);
    analytics.address.labels = vec!["analytics".into()];
    analytics.address.label_required = true;

    let replicas = LoadBalancer::new(
        &None,
        &[reporting, analytics],
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::ExcludePrimary,
        Default::default(),
    );
    replicas.launch();

    for (label, target) in [(None, 0), (Some("analytics"), 1)] {
        for _ in 0..4 {
            let conn = get_with_label(&replicas, label)
                .await
                .expect("eligible replica");
            assert_eq!(conn.pool.id(), replicas.targets[target].pool.id());
        }
    }

    replicas.shutdown();
}

#[tokio::test]
async fn test_label_filtering() {
    let replicas = labeled_replicas();
    for _ in 0..4 {
        let conn = get_with_label(&replicas, Some("analytics"))
            .await
            .expect("matching labeled replica");
        assert_eq!(conn.pool.id(), replicas.targets[1].pool.id());
    }

    replicas.shutdown();
}

#[tokio::test]
async fn test_label_respects_ban_when_unbanned_replica_exists() {
    let replicas = labeled_replicas();
    replicas.targets[0].ban.ban(Error::ManualBan, Duration::MAX);

    for _ in 0..4 {
        let conn = get_with_label(&replicas, Some("reporting"))
            .await
            .expect("unbanned labeled replica");
        assert_eq!(conn.pool.id(), replicas.targets[1].pool.id());
    }

    replicas.shutdown();
}

#[tokio::test]
async fn test_label_ignores_ban() {
    let replicas = labeled_replicas();
    let target = &replicas.targets[1];
    target.ban.ban(Error::ServerError, Duration::from_secs(60));
    assert!(target.ban.banned());

    let conn = get_with_label(&replicas, Some("analytics"))
        .await
        .expect("labeled request ignores the replica ban");
    assert_eq!(conn.pool.id(), target.pool.id());
    drop(conn);

    replicas.shutdown();
}

#[tokio::test]
async fn test_label_skips_banned_replica_when_matching_replica_is_available() {
    let replicas = labeled_replicas();
    replicas.targets[0]
        .ban
        .ban(Error::ServerError, Duration::from_secs(60));

    for _ in 0..4 {
        let conn = get_with_label(&replicas, Some("reporting"))
            .await
            .expect("unbanned matching replica");
        assert_eq!(conn.pool.id(), replicas.targets[1].pool.id());
        assert!(replicas.targets[0].ban.banned());
    }

    replicas.shutdown();
}

#[tokio::test]
async fn test_label_ignores_bans_when_all_matching_replicas_are_banned() {
    let replicas = labeled_replicas();
    for target in &replicas.targets[..2] {
        target.ban.ban(Error::ServerError, Duration::from_secs(60));
    }
    assert!(!replicas.targets[2].ban.banned());

    let mut selected = HashSet::new();
    for _ in 0..4 {
        let conn = get_with_label(&replicas, Some("reporting"))
            .await
            .expect("matching replica despite bans");
        selected.insert(conn.pool.id());
    }
    assert_eq!(
        selected,
        HashSet::from([replicas.targets[0].pool.id(), replicas.targets[1].pool.id()])
    );
    assert!(
        replicas.targets[..2]
            .iter()
            .all(|target| target.ban.banned())
    );

    replicas.shutdown();
}
