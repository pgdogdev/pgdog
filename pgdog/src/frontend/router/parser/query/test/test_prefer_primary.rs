use pgdog_config::{ConfigAndUsers, QueryParserLevel, ReadWriteSplit};

use crate::net::{Bind, Execute, Parse, ProtocolMessage, Query, Sync};

use super::setup::*;

/// With `prefer_primary` enabled, a plain read routes to the primary.
#[test]
fn test_prefer_primary_routes_read_to_primary() {
    let mut test = QueryParserTest::new().with_rw_split(ReadWriteSplit::PreferPrimary);

    let command = test.execute(vec![Query::new("SELECT * FROM users").into()]);

    assert!(command.route().is_write());
}

/// Baseline: without `prefer_primary`, a plain read still routes to a replica.
#[test]
fn test_prefer_primary_disabled_routes_read_to_replica() {
    let mut test = QueryParserTest::new().with_rw_split(ReadWriteSplit::default());

    let command = test.execute(vec![Query::new("SELECT * FROM users").into()]);

    assert!(command.route().is_read());
}

/// An explicit `pgdog.role = replica` parameter opts a read back onto replicas.
#[test]
fn test_prefer_primary_param_replica_overrides() {
    let mut test = QueryParserTest::new()
        .with_rw_split(ReadWriteSplit::PreferPrimary)
        .with_param("pgdog.role", "replica");

    let command = test.execute(vec![Query::new("SELECT * FROM users").into()]);

    assert!(command.route().is_read());
}

/// A `/* pgdog_role: replica */` query comment opts a read back onto replicas.
#[test]
fn test_prefer_primary_comment_replica_overrides() {
    let mut test = QueryParserTest::new().with_rw_split(ReadWriteSplit::PreferPrimary);

    let command = test.execute(vec![
        Query::new("/* pgdog_role: replica */ SELECT * FROM users").into(),
    ]);

    assert!(command.route().is_read());
}

/// Writes are unaffected: they still route to the primary.
#[test]
fn test_prefer_primary_write_routes_to_primary() {
    let mut test = QueryParserTest::new().with_rw_split(ReadWriteSplit::PreferPrimary);

    let command = test.execute(vec![Query::new("INSERT INTO users (id) VALUES (1)").into()]);

    assert!(command.route().is_write());
}

/// Readonly clusters have no primary: reads stay on replicas instead of failing with "no primary".
#[test]
fn test_prefer_primary_replica_only_cluster_routes_read_to_replica() {
    let mut config = ConfigAndUsers::default();
    config.config.general.query_parser = QueryParserLevel::On;

    let messages: [(&str, Vec<ProtocolMessage>); 3] = [
        ("simple", vec![Query::new("SELECT * FROM users").into()]),
        (
            "extended",
            vec![
                Parse::new_anonymous("SELECT * FROM users").into(),
                Bind::new_statement("").into(),
                Execute::new().into(),
                Sync.into(),
            ],
        ),
        (
            "prepared",
            vec![
                Parse::named("q", "SELECT * FROM users").into(),
                Bind::new_statement("q").into(),
                Execute::new().into(),
                Sync.into(),
            ],
        ),
    ];

    for (protocol, request) in messages {
        let mut test = QueryParserTest::new_single_replica(&config)
            .with_rw_split(ReadWriteSplit::PreferPrimary);
        assert!(test.cluster().read_only());

        let command = test.execute(request);

        assert!(command.route().is_read(), "{protocol}: {command:?}");
    }
}
