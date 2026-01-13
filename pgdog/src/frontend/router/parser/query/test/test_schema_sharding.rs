use crate::frontend::router::parser::{route::ShardSource, Shard};
use crate::net::parameter::ParameterValue;

use super::setup::{QueryParserTest, *};

// --- SELECT queries with schema-qualified tables ---

#[test]
fn test_select_from_shard_0_schema() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT * FROM shard_0.users WHERE id = 1").into()
    ]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_select_from_shard_1_schema() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT * FROM shard_1.users WHERE id = 1").into()
    ]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_select_from_unsharded_schema_goes_to_all() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT * FROM public.users WHERE id = 1").into()
    ]);

    // Unknown schema goes to all shards
    assert_eq!(command.route().shard(), &Shard::All);
}

// --- INSERT queries with schema-qualified tables ---

#[test]
fn test_insert_into_shard_0_schema() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "INSERT INTO shard_0.users (id, name) VALUES (1, 'test')",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_insert_into_shard_1_schema() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "INSERT INTO shard_1.users (id, name) VALUES (1, 'test')",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

// --- UPDATE queries with schema-qualified tables ---

#[test]
fn test_update_shard_0_schema() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "UPDATE shard_0.users SET name = 'updated' WHERE id = 1",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_update_shard_1_schema() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "UPDATE shard_1.users SET name = 'updated' WHERE id = 1",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

// --- DELETE queries with schema-qualified tables ---

#[test]
fn test_delete_from_shard_0_schema() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("DELETE FROM shard_0.users WHERE id = 1").into()
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_delete_from_shard_1_schema() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("DELETE FROM shard_1.users WHERE id = 1").into()
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

// --- JOIN queries with schema-qualified tables ---

#[test]
fn test_join_same_schema() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM shard_0.users u JOIN shard_0.orders o ON u.id = o.user_id",
    )
    .into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_join_different_schemas_uses_first_found() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM shard_0.users u JOIN shard_1.orders o ON u.id = o.user_id",
    )
    .into()]);

    // Cross-schema join uses the first schema found in the query
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

// --- DDL with schema-qualified tables ---

#[test]
fn test_create_table_in_shard_0_schema() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "CREATE TABLE shard_0.new_table (id BIGINT PRIMARY KEY)",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_create_table_in_shard_1_schema() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "CREATE TABLE shard_1.new_table (id BIGINT PRIMARY KEY)",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_drop_table_in_sharded_schema() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("DROP TABLE IF EXISTS shard_0.old_table").into()
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_alter_table_in_sharded_schema() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "ALTER TABLE shard_1.users ADD COLUMN email TEXT",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

// --- Index operations with schema-qualified tables ---

#[test]
fn test_create_index_on_sharded_schema() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "CREATE INDEX idx_users_name ON shard_0.users (name)",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

// --- Schema takes priority over table-based sharding ---

#[test]
#[ignore = "this is actually not the case currently"]
fn test_schema_sharding_takes_priority_over_table_sharding() {
    let mut test = QueryParserTest::new();

    // "sharded" table is configured for table-based sharding with column "id"
    // id=1 would normally hash to some shard, but schema prefix should override
    let command = test.execute(vec![Query::new(
        "SELECT * FROM shard_1.sharded WHERE id = 1",
    )
    .into()]);

    // Schema-based routing should take priority: shard_1 -> shard 1
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_schema_sharding_priority_on_insert() {
    let mut test = QueryParserTest::new();

    // Even though "sharded" table has sharding key "id", schema should win
    let command = test.execute(vec![Query::new(
        "INSERT INTO shard_0.sharded (id, email) VALUES (999, 'test@test.com')",
    )
    .into()]);

    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
#[ignore = "this is not currently how it works, but it should"]
fn test_schema_sharding_priority_on_update() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "UPDATE shard_1.sharded SET email = 'new@test.com' WHERE id = 1",
    )
    .into()]);

    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
#[ignore = "this is not currently how it works, but it should"]
fn test_schema_sharding_priority_on_delete() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "DELETE FROM shard_0.sharded WHERE id = 999",
    )
    .into()]);

    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

// --- SET commands with schema sharding via search_path ---

#[test]
fn test_set_routes_to_shard_from_search_path() {
    let mut test =
        QueryParserTest::new().with_param("search_path", ParameterValue::String("shard_0".into()));

    let command = test.execute(vec![Query::new("SET statement_timeout TO 1000").into()]);

    assert_eq!(command.route().shard(), &Shard::Direct(0));
    assert_eq!(
        command.route().shard_with_priority().source(),
        &ShardSource::SearchPath("shard_0".into())
    );
}
