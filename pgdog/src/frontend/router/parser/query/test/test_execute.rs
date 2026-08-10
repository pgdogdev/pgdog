//! Routing tests for SQL-level `PREPARE`/`EXECUTE` statements.
//!
//! `EXECUTE` must be routed based on the statement behind the name. If that
//! statement is a write that only touches omnisharded tables, the results
//! are identical on all shards, so the response (e.g. `UPDATE <rows>`) must
//! be deduplicated across shards instead of aggregated.

use crate::frontend::router::parser::{Error, Shard};

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

/// Reads are not flagged: `EXECUTE` always routes as a write, and an
/// omnisharded write requires full shard coverage, which would reject
/// shard directives on statements that can't diverge the shards.
#[test]
fn test_execute_omni_select_not_omnisharded() {
    let mut test = QueryParserTest::new();
    test.execute(vec![
        Query::new("PREPARE sel AS SELECT * FROM sharded_omni WHERE id = $1").into(),
    ]);

    let command = test.execute(vec![Query::new("EXECUTE sel(1)").into()]);

    let route = command.route();
    assert_eq!(route.shard(), &Shard::All);
    assert!(!route.is_omnisharded());
}

#[test]
fn test_execute_values_not_omnisharded() {
    let mut test = QueryParserTest::new();
    test.execute(vec![Query::new("PREPARE vals AS VALUES (1), (2)").into()]);

    let command = test.execute(vec![Query::new("EXECUTE vals").into()]);

    assert!(!command.route().is_omnisharded());
}

/// A shard directive on `EXECUTE` of a read-only statement is allowed;
/// the statement can't diverge the shards.
#[test]
fn test_execute_omni_select_with_shard_directive() {
    let mut test = QueryParserTest::new();
    test.execute(vec![
        Query::new("PREPARE sel AS SELECT * FROM sharded_omni WHERE id = $1").into(),
    ]);

    let command = test.execute(vec![
        Query::new("/* pgdog_shard: 0 */ EXECUTE sel(1)").into(),
    ]);

    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

/// A shard directive on `EXECUTE` of an omnisharded write is rejected,
/// like on the equivalent direct statement: reaching only one shard
/// would silently diverge the table.
#[test]
fn test_execute_omni_write_with_shard_directive_rejected() {
    let mut test = QueryParserTest::new();
    test.execute(vec![
        Query::new("PREPARE upd AS UPDATE sharded_omni SET value = $1").into(),
    ]);

    let result = test.try_execute(vec![
        Query::new("/* pgdog_shard: 0 */ EXECUTE upd('x')").into(),
    ]);

    assert!(matches!(result, Err(Error::OmniWriteWithDirective)));
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
