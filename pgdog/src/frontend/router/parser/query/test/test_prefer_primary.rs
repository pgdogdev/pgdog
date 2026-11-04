use pgdog_config::ReadWriteSplit;

use crate::backend::pool::RoleTarget;

use super::setup::*;

/// With `prefer_primary` enabled, the route of a plain read stays `Any`.
/// The connection sends it to the primary.
#[test]
fn test_prefer_primary_read_leaves_choice_to_cluster() {
    let mut test = QueryParserTest::new().with_rw_split(ReadWriteSplit::PreferPrimary);

    let command = test.execute(vec![Query::new("SELECT * FROM users").into()]);

    assert_eq!(command.route().role_target(), RoleTarget::Any);
}

/// An explicit `pgdog.role = replica` parameter opts a read back onto replicas.
#[test]
fn test_prefer_primary_param_replica_overrides() {
    let mut test = QueryParserTest::new()
        .with_rw_split(ReadWriteSplit::PreferPrimary)
        .with_param("pgdog.role", "replica");

    let command = test.execute(vec![Query::new("SELECT * FROM users").into()]);

    assert_eq!(command.route().role_target(), RoleTarget::PreferReplica);
}

/// A `/* pgdog_role: replica */` query comment opts a read back onto replicas.
#[test]
fn test_prefer_primary_comment_replica_overrides() {
    let mut test = QueryParserTest::new().with_rw_split(ReadWriteSplit::PreferPrimary);

    let command = test.execute(vec![
        Query::new("/* pgdog_role: replica */ SELECT * FROM users").into(),
    ]);

    assert_eq!(command.route().role_target(), RoleTarget::PreferReplica);
}

/// Writes go to the primary only.
#[test]
fn test_prefer_primary_write_routes_to_primary() {
    let mut test = QueryParserTest::new().with_rw_split(ReadWriteSplit::PreferPrimary);

    let command = test.execute(vec![Query::new("INSERT INTO users (id) VALUES (1)").into()]);

    assert_eq!(command.route().role_target(), RoleTarget::Primary);
}
