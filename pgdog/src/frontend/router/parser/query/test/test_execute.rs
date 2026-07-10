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
