use crate::frontend::router::parser::route::{RoundRobinReason, TableReason};
use crate::frontend::router::parser::{route::ShardSource, Shard};
use crate::frontend::{router::parser::Route, Command};
use crate::net::parameter::ParameterValue;

use super::setup::{QueryParserTest, *};

// --- search_path routing for DML ---

fn tuple(path: &[&str]) -> ParameterValue {
    ParameterValue::Tuple(path.iter().map(|schema| (*schema).into()).collect())
}

fn assert_search_path_route(route: &Route, shard: usize, schema: &str) {
    assert_eq!(route.shard(), &Shard::Direct(shard));
    assert!(route.is_search_path_driven());
    assert_eq!(
        route.shard_with_priority().source(),
        &ShardSource::SearchPath(schema.into())
    );
}

fn assert_sharded_broadcast(command: &Command) {
    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::All);
    assert_eq!(
        command.route().shard_with_priority().source(),
        &ShardSource::Table(TableReason::Sharded)
    );
}

fn assert_omni_write(command: &Command, source: ShardSource) {
    assert!(command.route().is_write());
    assert!(command.route().is_omni());
    assert_eq!(command.route().shard_with_priority().source(), &source);
}

#[test]
fn test_search_path_shard_0_routes_select() {
    let mut test =
        QueryParserTest::new().with_param("search_path", tuple(&["$user", "shard_0", "public"]));

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_search_path_shard_0_routes_select_without_shard_key() {
    let mut test =
        QueryParserTest::new().with_param("search_path", tuple(&["$user", "shard_0", "public"]));

    let command = test.execute(vec![Query::new("SELECT * FROM users").into()]);

    assert!(command.route().is_read());
    assert_search_path_route(command.route(), 0, "shard_0");
}

#[test]
fn test_search_path_shard_1_routes_select() {
    let mut test =
        QueryParserTest::new().with_param("search_path", tuple(&["public", "shard_1", "$user"]));

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_search_path_shard_0_routes_insert() {
    let mut test =
        QueryParserTest::new().with_param("search_path", tuple(&["pg_catalog", "shard_0"]));

    let command = test.execute(vec![Query::new(
        "INSERT INTO users (id, name) VALUES (1, 'test')",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_search_path_shard_0_routes_insert_without_shard_key() {
    let mut test =
        QueryParserTest::new().with_param("search_path", tuple(&["pg_catalog", "shard_0"]));

    let command = test.execute(vec![
        Query::new("INSERT INTO users (name) VALUES ('test')").into()
    ]);

    assert_search_path_route(command.route(), 0, "shard_0");
}

#[test]
fn test_search_path_shard_1_routes_insert() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        tuple(&["$user", "public", "pg_temp", "shard_1"]),
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
    let mut test = QueryParserTest::new()
        .with_param("search_path", tuple(&["shard_0", "pg_catalog", "public"]));

    let command = test.execute(vec![Query::new(
        "UPDATE users SET name = 'updated' WHERE id = 1",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_search_path_shard_0_routes_update_without_shard_key() {
    let mut test = QueryParserTest::new()
        .with_expanded_explain()
        .with_param("search_path", tuple(&["shard_0", "pg_catalog", "public"]));

    let command = test.execute(vec![Query::new("UPDATE users SET name = 'updated'").into()]);

    assert_search_path_route(command.route(), 0, "shard_0");
}

#[test]
fn test_search_path_shard_1_routes_update() {
    let mut test =
        QueryParserTest::new().with_param("search_path", tuple(&["information_schema", "shard_1"]));

    let command = test.execute(vec![Query::new(
        "UPDATE users SET name = 'updated' WHERE id = 1",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_search_path_shard_0_routes_delete() {
    let mut test =
        QueryParserTest::new().with_param("search_path", tuple(&["public", "$user", "shard_0"]));

    let command = test.execute(vec![Query::new("DELETE FROM users WHERE id = 1").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_search_path_shard_0_routes_delete_without_shard_key() {
    let mut test = QueryParserTest::new()
        .with_expanded_explain()
        .with_param("search_path", tuple(&["public", "$user", "shard_0"]));

    let command = test.execute(vec![Query::new("DELETE FROM users").into()]);

    assert_search_path_route(command.route(), 0, "shard_0");
}

#[test]
fn test_search_path_shard_1_routes_delete() {
    let mut test = QueryParserTest::new().with_param("search_path", tuple(&["shard_1", "public"]));

    let command = test.execute(vec![Query::new("DELETE FROM users WHERE id = 1").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

// --- search_path routing priority (first sharded schema wins) ---

#[test]
fn test_search_path_first_shard_wins_shard_0() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        tuple(&["public", "shard_0", "shard_1", "$user"]),
    );

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_search_path_first_shard_wins_shard_1() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        tuple(&["$user", "pg_catalog", "shard_1", "shard_0"]),
    );

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_search_path_shard_at_end_still_matches() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        tuple(&[
            "$user",
            "public",
            "pg_catalog",
            "information_schema",
            "shard_1",
        ]),
    );

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

// --- search_path with no sharded schema routes to all ---

#[test]
fn test_search_path_no_sharded_schema_routes_to_rr() {
    let mut test =
        QueryParserTest::new().with_param("search_path", tuple(&["$user", "public", "pg_catalog"]));

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(matches!(command.route().shard(), &Shard::Direct(_)));
    assert_eq!(
        command.route().shard_with_priority().source(),
        &ShardSource::RoundRobin(RoundRobinReason::Omni)
    );
}

#[test]
fn test_no_search_path_ambiguous_insert_routes_to_all() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("INSERT INTO orders DEFAULT VALUES").into()]);

    assert_sharded_broadcast(&command);
}

#[test]
fn test_sharded_update_without_shard_key_routes_to_all() {
    let mut test = QueryParserTest::new().with_expanded_explain();

    let command = test.execute(vec![
        Query::new("UPDATE sharded SET email = 'updated'").into()
    ]);

    assert_sharded_broadcast(&command);
}

#[test]
fn test_sharded_delete_without_shard_key_routes_to_all() {
    let mut test = QueryParserTest::new().with_expanded_explain();

    let command = test.execute(vec![Query::new("DELETE FROM sharded").into()]);

    assert_sharded_broadcast(&command);
}

#[test]
fn test_public_schema_insert_routes_to_omni() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "INSERT INTO public.users (name) VALUES ('test')",
    )
    .into()]);

    assert_omni_write(&command, ShardSource::Table(TableReason::Omni));
}

#[test]
fn test_public_schema_update_routes_to_omni() {
    let mut test = QueryParserTest::new().with_expanded_explain();

    let command = test.execute(vec![
        Query::new("UPDATE public.users SET name = 'updated'").into()
    ]);

    assert_omni_write(&command, ShardSource::Table(TableReason::Omni));
}

#[test]
fn test_public_schema_delete_routes_to_omni() {
    let mut test = QueryParserTest::new().with_expanded_explain();

    let command = test.execute(vec![Query::new("DELETE FROM public.users").into()]);

    assert_omni_write(&command, ShardSource::RoundRobin(RoundRobinReason::Omni));
}

#[test]
fn test_search_path_only_system_schemas_routes_to_rr() {
    let mut test = QueryParserTest::new().with_param(
        "search_path",
        tuple(&["pg_catalog", "information_schema", "pg_temp"]),
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
