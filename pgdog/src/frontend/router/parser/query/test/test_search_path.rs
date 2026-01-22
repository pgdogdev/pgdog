use crate::frontend::router::parser::route::RoundRobinReason;
use crate::frontend::router::parser::{route::ShardSource, Shard};
use crate::net::parameter::ParameterValue;

use super::setup::{QueryParserTest, *};

// --- search_path routing for DML ---

#[test]
fn test_search_path_shard_0_routes_select() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec!["$user".into(), "shard_0".into(), "public".into()]),
    );

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_search_path_shard_1_routes_select() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec!["public".into(), "shard_1".into(), "$user".into()]),
    );

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_search_path_shard_0_routes_insert() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec!["pg_catalog".into(), "shard_0".into()]),
    );

    let command = test.execute(vec![Query::new(
        "INSERT INTO users (id, name) VALUES (1, 'test')",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_search_path_shard_1_routes_insert() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec![
            "$user".into(),
            "public".into(),
            "pg_temp".into(),
            "shard_1".into(),
        ]),
    );

    let command = test.execute(vec![Query::new(
        "INSERT INTO users (id, name) VALUES (1, 'test')",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_search_path_shard_0_routes_update() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec!["shard_0".into(), "pg_catalog".into(), "public".into()]),
    );

    let command = test.execute(vec![Query::new(
        "UPDATE users SET name = 'updated' WHERE id = 1",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_search_path_shard_1_routes_update() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec!["information_schema".into(), "shard_1".into()]),
    );

    let command = test.execute(vec![Query::new(
        "UPDATE users SET name = 'updated' WHERE id = 1",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_search_path_shard_0_routes_delete() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec!["public".into(), "$user".into(), "shard_0".into()]),
    );

    let command = test.execute(vec![Query::new("DELETE FROM users WHERE id = 1").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_search_path_shard_1_routes_delete() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec!["shard_1".into(), "public".into()]),
    );

    let command = test.execute(vec![Query::new("DELETE FROM users WHERE id = 1").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

// --- search_path routing priority (first sharded schema wins) ---

#[test]
fn test_search_path_first_shard_wins_shard_0() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec![
            "public".into(),
            "shard_0".into(),
            "shard_1".into(),
            "$user".into(),
        ]),
    );

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_search_path_first_shard_wins_shard_1() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec![
            "$user".into(),
            "pg_catalog".into(),
            "shard_1".into(),
            "shard_0".into(),
        ]),
    );

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_search_path_shard_at_end_still_matches() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec![
            "$user".into(),
            "public".into(),
            "pg_catalog".into(),
            "information_schema".into(),
            "shard_1".into(),
        ]),
    );

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

// --- search_path with no sharded schema routes to all ---

#[test]
fn test_search_path_no_sharded_schema_routes_to_rr() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec!["$user".into(), "public".into(), "pg_catalog".into()]),
    );

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(matches!(command.route().shard(), &Shard::Direct(_)));
    assert_eq!(
        command.route().shard_with_priority().source(),
        &ShardSource::RoundRobin(RoundRobinReason::Omni)
    );
}

#[test]
fn test_search_path_only_system_schemas_routes_to_rr() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec![
            "pg_catalog".into(),
            "information_schema".into(),
            "pg_temp".into(),
        ]),
    );

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(matches!(command.route().shard(), &Shard::Direct(_)));
    assert_eq!(
        command.route().shard_with_priority().source(),
        &ShardSource::RoundRobin(RoundRobinReason::Omni)
    );
}

// --- search_path routing for DDL ---

#[test]
fn test_search_path_shard_0_routes_create_table() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec!["$user".into(), "shard_0".into(), "public".into()]),
    );

    let command = test.execute(vec![Query::new(
        "CREATE TABLE new_table (id SERIAL PRIMARY KEY)",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_search_path_shard_1_routes_create_table() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec!["public".into(), "pg_catalog".into(), "shard_1".into()]),
    );

    let command = test.execute(vec![Query::new(
        "CREATE TABLE new_table (id SERIAL PRIMARY KEY)",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_search_path_shard_0_routes_drop_table() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec!["pg_temp".into(), "$user".into(), "shard_0".into()]),
    );

    let command = test.execute(vec![Query::new("DROP TABLE IF EXISTS old_table").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_search_path_shard_1_routes_alter_table() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec!["shard_1".into(), "$user".into(), "public".into()]),
    );

    let command = test.execute(vec![
        Query::new("ALTER TABLE users ADD COLUMN email TEXT").into()
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

// --- search_path driven flag ---

#[test]
fn test_search_path_driven_flag_is_set() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec!["$user".into(), "public".into(), "shard_0".into()]),
    );

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_search_path_driven());
}

#[test]
fn test_search_path_driven_flag_not_set_for_no_sharded_schema() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        ParameterValue::Tuple(vec!["$user".into(), "public".into(), "pg_catalog".into()]),
    );

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(!command.route().is_search_path_driven());
}
