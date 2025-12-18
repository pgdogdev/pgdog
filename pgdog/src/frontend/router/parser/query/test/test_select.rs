use crate::frontend::router::parser::{DistinctBy, DistinctColumn, Shard};
use crate::net::messages::Parameter;

use super::setup::*;

#[test]
fn test_order_by_vector_simple() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM embeddings ORDER BY embedding <-> '[1,2,3]'",
    )
    .into()]);

    let route = command.route();
    let order_by = route.order_by().first().unwrap();
    assert!(order_by.asc());
}

#[test]
fn test_order_by_vector_with_params() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named(
            "__test_order",
            "SELECT * FROM embeddings ORDER BY embedding <-> $1",
        )
        .into(),
        Bind::new_params("__test_order", &[Parameter::new(b"[4.0,5.0,6.0]")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let route = command.route();
    let order_by = route.order_by().first().unwrap();
    assert!(order_by.asc());
}

#[test]
fn test_limit_offset_simple() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT * FROM users LIMIT 25 OFFSET 5").into()
    ]);

    let route = command.route();
    assert_eq!(route.limit().offset, Some(5));
    assert_eq!(route.limit().limit, Some(25));
}

#[test]
fn test_limit_offset_with_params() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named("__test_limit", "SELECT * FROM users LIMIT $1 OFFSET $2").into(),
        Bind::new_params(
            "__test_limit",
            &[Parameter::new(b"1"), Parameter::new(b"25")],
        )
        .into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let route = command.route();
    assert_eq!(route.limit().limit, Some(1));
    assert_eq!(route.limit().offset, Some(25));
}

#[test]
fn test_distinct_row() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT DISTINCT * FROM users").into()]);

    let route = command.route();
    let distinct = route.distinct().as_ref().unwrap();
    assert_eq!(distinct, &DistinctBy::Row);
}

#[test]
fn test_distinct_on_columns() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT DISTINCT ON(1, email) * FROM users",
    )
    .into()]);

    let route = command.route();
    let distinct = route.distinct().as_ref().unwrap();
    assert_eq!(
        distinct,
        &DistinctBy::Columns(vec![
            DistinctColumn::Index(0),
            DistinctColumn::Name(String::from("email"))
        ])
    );
}

#[test]
fn test_any_literal() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded WHERE id = ANY('{1, 2, 3}')",
    )
    .into()]);

    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_any_with_param() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named("__test_any", "SELECT * FROM sharded WHERE id = ANY($1)").into(),
        Bind::new_params("__test_any", &[Parameter::new(b"{1, 2, 3}")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_cte_read() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("WITH s AS (SELECT 1) SELECT 2").into()]);

    assert!(command.route().is_read());
}

#[test]
fn test_cte_write() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "WITH s AS (SELECT 1), s2 AS (INSERT INTO test VALUES ($1) RETURNING *), s3 AS (SELECT 123) SELECT * FROM s",
    )
    .into()]);

    assert!(command.route().is_write());
}
