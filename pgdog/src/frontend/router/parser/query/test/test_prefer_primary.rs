use super::setup::*;

/// With `prefer_primary` enabled, a plain read routes to the primary.
#[test]
fn test_prefer_primary_routes_read_to_primary() {
    let mut test = QueryParserTest::new().with_prefer_primary(true);

    let command = test.execute(vec![Query::new("SELECT * FROM users").into()]);

    assert!(command.route().is_write());
}

/// Baseline: without `prefer_primary`, a plain read still routes to a replica.
#[test]
fn test_prefer_primary_disabled_routes_read_to_replica() {
    let mut test = QueryParserTest::new().with_prefer_primary(false);

    let command = test.execute(vec![Query::new("SELECT * FROM users").into()]);

    assert!(command.route().is_read());
}

/// An explicit `pgdog.role = replica` parameter opts a read back onto replicas.
#[test]
fn test_prefer_primary_param_replica_overrides() {
    let mut test = QueryParserTest::new()
        .with_prefer_primary(true)
        .with_param("pgdog.role", "replica");

    let command = test.execute(vec![Query::new("SELECT * FROM users").into()]);

    assert!(command.route().is_read());
}

/// A `/* pgdog_role: replica */` query comment opts a read back onto replicas.
#[test]
fn test_prefer_primary_comment_replica_overrides() {
    let mut test = QueryParserTest::new().with_prefer_primary(true);

    let command = test.execute(vec![
        Query::new("/* pgdog_role: replica */ SELECT * FROM users").into(),
    ]);

    assert!(command.route().is_read());
}

/// Writes are unaffected: they still route to the primary.
#[test]
fn test_prefer_primary_write_routes_to_primary() {
    let mut test = QueryParserTest::new().with_prefer_primary(true);

    let command = test.execute(vec![Query::new("INSERT INTO users (id) VALUES (1)").into()]);

    assert!(command.route().is_write());
}
