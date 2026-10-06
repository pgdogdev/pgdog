use super::*;

fn auto_pool(hosts: &[&str]) -> LoadBalancer {
    let configs: Vec<_> = hosts
        .iter()
        .map(|host| create_auto_test_pool_config(host, 5432))
        .collect();
    LoadBalancer::new(
        &None,
        &configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
        Default::default(),
    )
}

#[test]
fn test_roles_detected_waits_for_all_replicas() {
    let lb = auto_pool(&["localhost", "127.0.0.1"]);
    assert!(lb.has_replicas(), "reads can start before role detection");
    assert!(!lb.roles_detected());
    assert!(!lb.redetect_roles());
    assert!(
        !lb.roles_detected(),
        "targets without LSN stats remain unknown"
    );

    set_lsn_stats(&lb.targets[0], true, 100);
    assert!(!lb.redetect_roles());
    assert!(!lb.roles_detected(), "the other target could be a primary");
    assert!(
        lb.targets[0].role_detected.load(Ordering::Acquire),
        "valid replica stats resolve its role before other targets"
    );
    assert!(
        !lb.targets[1].role_detected.load(Ordering::Acquire),
        "target without LSN stats remains unknown"
    );

    set_lsn_stats(&lb.targets[1], true, 100);
    assert!(!lb.redetect_roles());
    assert!(lb.roles_detected());
    assert!(lb.clone().roles_detected(), "clones share detection state");
    assert!(lb.primary().is_none());

    set_lsn_stats(&lb.targets[1], false, 200);
    assert!(lb.redetect_roles());
    assert!(lb.roles_detected());
    assert_eq!(
        lb.primary().expect("elected primary").addr().host,
        "127.0.0.1"
    );

    set_lsn_stats(&lb.targets[1], true, 200);
    lb.redetect_roles();
    assert!(lb.roles_detected());
    assert!(lb.primary().is_none());
}

#[test]
fn test_roles_detected_when_primary_found_before_other_targets() {
    let lb = auto_pool(&["localhost", "127.0.0.1"]);
    set_lsn_stats(&lb.targets[0], false, 100);
    assert!(lb.redetect_roles());
    assert!(lb.roles_detected());
    assert!(lb.primary().is_some());
}

#[tokio::test]
async fn test_roles_detected_survives_reload_and_new_targets_remain_unknown() {
    let old = auto_pool(&["localhost", "127.0.0.1"]);
    set_lsn_stats(&old.targets[0], true, 100);
    set_lsn_stats(&old.targets[1], true, 100);
    old.redetect_roles();

    let same = auto_pool(&["127.0.0.1", "localhost"]);
    old.move_conns_to(&same).expect("transfer pools");
    assert!(
        same.roles_detected(),
        "known roles survive a reordered reload"
    );

    let expanded = auto_pool(&["localhost", "127.0.0.1", "new-replica"]);
    same.move_conns_to(&expanded)
        .expect("transfer existing pools");
    assert!(!expanded.roles_detected(), "new target needs detection");
    expanded.redetect_roles();
    assert!(
        !expanded.roles_detected(),
        "new target still needs LSN stats"
    );

    let reloaded = auto_pool(&["localhost", "127.0.0.1", "new-replica"]);
    expanded
        .move_conns_to(&reloaded)
        .expect("transfer unknown target too");
    assert!(
        !reloaded.roles_detected(),
        "copying a provisional replica does not resolve it"
    );

    let reduced = auto_pool(&["localhost", "127.0.0.1"]);
    reloaded
        .move_conns_to(&reduced)
        .expect("remove unknown target");
    assert!(reduced.roles_detected());

    set_lsn_stats(&reloaded.targets[2], true, 100);
    reloaded.redetect_roles();
    assert!(reloaded.roles_detected());
}

#[test]
fn test_redetect_roles_prefers_highest_timeline_then_freshest_stats() {
    for (first_timeline, second_timeline, expected_primary) in [
        (1, 2, "127.0.0.1"),
        (2, 2, "localhost"),
        (0, 0, "localhost"),
    ] {
        let lb = auto_pool(&["localhost", "127.0.0.1"]);
        let now = SystemTime::now();
        for (target, timeline, age) in [
            (&lb.targets[0], first_timeline, Duration::ZERO),
            (&lb.targets[1], second_timeline, Duration::from_secs(10)),
        ] {
            set_lsn_stats(target, false, 100);
            let mut stats = target.pool.inner().lsn_stats.write();
            stats.timeline = timeline;
            stats.fetched = now - age;
        }

        assert!(lb.redetect_roles());
        assert_eq!(
            lb.primary().expect("elected primary").addr().host,
            expected_primary,
            "timelines: {first_timeline}, {second_timeline}"
        );
    }
}
