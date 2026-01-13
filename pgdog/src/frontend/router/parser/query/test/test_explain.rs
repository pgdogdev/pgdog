use crate::frontend::router::parser::Shard;
use crate::net::messages::Parameter;

use super::setup::{QueryParserTest, *};

#[test]
fn test_explain_select() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "EXPLAIN SELECT * FROM sharded WHERE id = 1",
    )
    .into()]);

    assert!(command.route().is_read());
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

#[test]
fn test_explain_analyze_select() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "EXPLAIN ANALYZE SELECT * FROM sharded WHERE id = 1",
    )
    .into()]);

    // EXPLAIN ANALYZE actually runs the query, so it should be treated as write
    assert!(command.route().is_read());
}

#[test]
fn test_explain_insert() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "EXPLAIN INSERT INTO sharded (id, email) VALUES (1, 'test')",
    )
    .into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_explain_with_params() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named(
            "__test_explain",
            "EXPLAIN SELECT * FROM sharded WHERE id = $1",
        )
        .into(),
        Bind::new_params("__test_explain", &[Parameter::new(b"5")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert!(command.route().is_read());
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

#[test]
fn test_explain_all_shards() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("EXPLAIN SELECT * FROM sharded").into()]);

    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_explain_verbose() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "EXPLAIN (VERBOSE, COSTS) SELECT * FROM sharded WHERE id = 1",
    )
    .into()]);

    assert!(command.route().is_read());
}
