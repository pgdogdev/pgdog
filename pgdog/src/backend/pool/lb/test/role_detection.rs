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
    assert!(!lb.roles_detected());

    set_lsn_stats(&lb.targets[0], true, 100);
    assert!(!lb.redetect_roles());
    assert!(!lb.roles_detected(), "the other target could be a primary");

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
    assert!(!expanded.roles_detected());

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

/// Stats of a server that reports pg_is_in_recovery() = false, fetched `age` ago.
fn set_primary_stats(target: &Target, timeline: i64, lsn: i64, age: Duration) {
    let stats: crate::backend::pool::lsn_monitor::LsnStats = StatsLsnStats {
        replica: false,
        lsn: Lsn::from_i64(lsn),
        offset_bytes: lsn,
        fetched: std::time::SystemTime::now() - age,
        timeline,
        ..Default::default()
    }
    .into();
    *target.pool.inner().lsn_stats.write() = stats;
}

fn primary_host(lb: &LoadBalancer) -> Option<String> {
    lb.primary().map(|pool| pool.addr().host.clone())
}

#[test]
fn test_old_primary_back_on_old_timeline_loses_election() {
    let lb = auto_pool(&["localhost", "127.0.0.1"]);
    set_primary_stats(&lb.targets[0], 1, 500, Duration::ZERO);
    set_lsn_stats(&lb.targets[1], true, 500);
    assert!(lb.redetect_roles());
    assert_eq!(primary_host(&lb).as_deref(), Some("localhost"));

    // Failover: 127.0.0.1 is promoted and starts timeline 2.
    set_primary_stats(&lb.targets[1], 2, 520, Duration::from_secs(1));

    // The old primary restarts before it's turned into a replica. It
    // accepts writes on timeline 1: more WAL, fresher stats. It must
    // not win, or writes flap between two primaries.
    set_primary_stats(&lb.targets[0], 1, 9_000, Duration::ZERO);

    assert!(lb.redetect_roles());
    assert_eq!(primary_host(&lb).as_deref(), Some("127.0.0.1"));
    assert_eq!(lb.targets[0].role(), Role::Replica);

    assert!(!lb.redetect_roles(), "the election is stable");
    assert_eq!(primary_host(&lb).as_deref(), Some("127.0.0.1"));
}

#[test]
fn test_promoted_replica_beats_crashed_primary_with_more_wal() {
    let lb = auto_pool(&["localhost", "127.0.0.1"]);
    set_primary_stats(&lb.targets[0], 1, 900, Duration::ZERO);
    set_lsn_stats(&lb.targets[1], true, 800);
    assert!(lb.redetect_roles());

    // The primary stops answering and keeps its last stats. The replica
    // is promoted before it replayed everything the primary wrote.
    set_primary_stats(&lb.targets[0], 1, 900, Duration::from_secs(60));
    set_primary_stats(&lb.targets[1], 2, 800, Duration::ZERO);

    assert!(lb.redetect_roles());
    assert_eq!(primary_host(&lb).as_deref(), Some("127.0.0.1"));
}

#[test]
fn test_primary_in_recovery_is_demoted_while_another_target_is_unknown() {
    let lb = auto_pool(&["localhost", "127.0.0.1", "unknown-replica"]);
    set_primary_stats(&lb.targets[0], 1, 500, Duration::ZERO);
    set_lsn_stats(&lb.targets[1], true, 500);
    assert!(lb.redetect_roles());
    assert_eq!(primary_host(&lb).as_deref(), Some("localhost"));

    // The primary comes back as a replica (e.g. after a failover) while
    // the third server hasn't answered an LSN check yet. Nobody else is
    // the primary: writes must stop going to a read-only server.
    set_lsn_stats(&lb.targets[0], true, 510);
    assert!(!lb.redetect_roles(), "nobody was promoted");

    assert!(lb.primary().is_none());
    assert_eq!(lb.targets[0].role(), Role::Replica);
}

#[test]
fn test_aurora_election_prefers_freshest_stats() {
    let lb = auto_pool(&["localhost", "127.0.0.1"]);

    // Aurora reports neither a timeline nor an LSN.
    for (target, age) in lb.targets.iter().zip([60, 0]) {
        let stats: crate::backend::pool::lsn_monitor::LsnStats = StatsLsnStats {
            replica: false,
            aurora: true,
            fetched: std::time::SystemTime::now() - Duration::from_secs(age),
            ..Default::default()
        }
        .into();
        *target.pool.inner().lsn_stats.write() = stats;
    }

    assert!(lb.redetect_roles());
    assert_eq!(primary_host(&lb).as_deref(), Some("127.0.0.1"));
}
