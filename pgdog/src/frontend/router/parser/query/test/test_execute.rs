//! Routing tests for SQL-level `PREPARE`/`EXECUTE` statements.
//!
//! `EXECUTE` must be routed based on the statement behind the name. If that
//! statement only touches omnisharded tables, the results are identical on
//! all shards, so the response (e.g. `UPDATE <rows>`) must be deduplicated
//! across shards instead of aggregated.

use crate::frontend::PreparedStatements;
use crate::frontend::router::parser::Shard;

use super::setup::{QueryParserTest, *};

#[test]
fn test_execute_omni_update_is_omnisharded() {
    let mut test = QueryParserTest::new();
    test.execute(vec![
        Query::new("PREPARE upd AS UPDATE sharded_omni SET value = $1").into(),
    ]);

    let command = test.execute(vec![Query::new("EXECUTE upd('x')").into()]);

    let route = command.route();
    assert!(route.is_write());
    assert_eq!(route.shard(), &Shard::All);
    assert!(
        route.is_omnisharded(),
        "EXECUTE of an omnisharded UPDATE must carry the omnisharded flag, got {:?}",
        route
    );
}

#[test]
fn test_execute_omni_delete_is_omnisharded() {
    let mut test = QueryParserTest::new();
    test.execute(vec![
        Query::new("PREPARE del AS DELETE FROM sharded_omni WHERE id = $1").into(),
    ]);

    let command = test.execute(vec![Query::new("EXECUTE del(1)").into()]);

    let route = command.route();
    assert!(route.is_write());
    assert_eq!(route.shard(), &Shard::All);
    assert!(
        route.is_omnisharded(),
        "EXECUTE of an omnisharded DELETE must carry the omnisharded flag, got {:?}",
        route
    );
}

#[test]
fn test_execute_omni_select_is_omnisharded() {
    let mut test = QueryParserTest::new();
    test.execute(vec![
        Query::new("PREPARE sel AS SELECT * FROM sharded_omni WHERE id = $1").into(),
    ]);

    let command = test.execute(vec![Query::new("EXECUTE sel(1)").into()]);

    let route = command.route();
    assert_eq!(route.shard(), &Shard::All);
    assert!(route.is_omnisharded());
}

#[test]
fn test_execute_omni_insert_is_omnisharded() {
    let mut test = QueryParserTest::new();
    test.execute(vec![
        Query::new("PREPARE ins AS INSERT INTO sharded_omni (id, value) VALUES ($1, $2)").into(),
    ]);

    let command = test.execute(vec![Query::new("EXECUTE ins(1, 'a')").into()]);

    let route = command.route();
    assert_eq!(route.shard(), &Shard::All);
    assert!(route.is_omnisharded());
}

#[test]
fn test_execute_without_omnisharded_tables_not_omnisharded() {
    let mut test = QueryParserTest::new().with_sharded_tables(crate::backend::ShardedTables::new(
        vec![],
        vec![],
        false,
        Default::default(),
    ));
    test.execute(vec![
        Query::new("PREPARE upd AS UPDATE sharded_omni SET value = $1").into(),
    ]);

    let command = test.execute(vec![Query::new("EXECUTE upd('x')").into()]);

    assert!(!command.route().is_omnisharded());
}

#[test]
fn test_prepare_routes_to_all_shards() {
    let mut test = QueryParserTest::new();
    let command = test.execute(vec![
        Query::new("PREPARE upd AS UPDATE sharded_omni SET value = $1").into(),
    ]);

    let route = command.route();
    assert!(route.is_write());
    assert_eq!(route.shard(), &Shard::All);
}

#[test]
fn test_execute_sharded_table_not_omnisharded() {
    let mut test = QueryParserTest::new();
    test.execute(vec![
        Query::new("PREPARE upd AS UPDATE sharded SET value = $1").into(),
    ]);

    let command = test.execute(vec![Query::new("EXECUTE upd('x')").into()]);

    let route = command.route();
    assert!(route.is_write());
    assert_eq!(route.shard(), &Shard::All);
    assert!(!route.is_omnisharded());
}

#[test]
fn test_execute_unknown_statement_not_omnisharded() {
    let mut test = QueryParserTest::new();
    let command = test.execute(vec![Query::new("EXECUTE not_prepared(1)").into()]);

    let route = command.route();
    assert!(route.is_write());
    assert_eq!(route.shard(), &Shard::All);
    assert!(!route.is_omnisharded());
}

/// Statements that resolve but can't be routed by table are not omnisharded.
#[test]
fn test_execute_unroutable_statements_not_omnisharded() {
    for query in [
        "NOT VALID SQL",
        "-- just a comment",
        "CREATE TABLE t (id BIGINT)",
    ] {
        let parse = Parse::new_anonymous(query);
        let name = PreparedStatements::global().write().insert_anyway(&parse);

        let mut test = QueryParserTest::new();
        let command = test.execute(vec![Query::new(format!("EXECUTE {}(1)", name)).into()]);

        assert!(
            !command.route().is_omnisharded(),
            "{:?} must not be omnisharded",
            query
        );
    }
}

/// With `prepared_statements = "full"`, `EXECUTE` names are rewritten to
/// globally unique names before routing. Those resolve through the global
/// prepared statements cache instead.
#[test]
fn test_execute_global_name_resolves_through_global_cache() {
    let parse = Parse::new_anonymous("DELETE FROM sharded_omni WHERE id = $1");
    let name = PreparedStatements::global().write().insert_anyway(&parse);

    let mut test = QueryParserTest::new();
    let command = test.execute(vec![Query::new(format!("EXECUTE {}(1)", name)).into()]);

    let route = command.route();
    assert!(route.is_write());
    assert_eq!(route.shard(), &Shard::All);
    assert!(route.is_omnisharded());
}
