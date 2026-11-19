use super::*;

fn labeled_replicas() -> LoadBalancer {
    let mut reporting = create_test_pool_config("127.0.0.1", 5432);
    reporting.address.labels = vec!["reporting".into()];
    let mut analytics = create_test_pool_config("localhost", 5432);
    analytics.address.labels = vec!["reporting".into(), "analytics".into()];

    let replicas = LoadBalancer::new(
        &None,
        &[reporting, analytics],
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::ExcludePrimary,
        Default::default(),
    );
    replicas.launch();
    replicas
}

#[tokio::test]
async fn test_label_filtering() {
    let replicas = labeled_replicas();
    let mut route = Route::read(ShardWithPriority::default());
    route.label = Some("analytics".into());
    let request = LbRequest::new(Default::default(), &route, false, Instant::now());

    for _ in 0..4 {
        let conn = replicas
            .get(&request)
            .await
            .expect("matching labeled replica");
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

    let mut route = Route::read(ShardWithPriority::default());
    route.label = Some("analytics".into());
    let request = LbRequest::new(Default::default(), &route, false, Instant::now());

    let conn = replicas
        .get(&request)
        .await
        .expect("labeled request ignores the replica ban");
    assert_eq!(conn.pool.id(), target.pool.id());
    drop(conn);

    replicas.shutdown();
}
