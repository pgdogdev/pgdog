use crate::frontend::router::parser::Shard;
use crate::net::messages::Parameter;

use super::setup::{QueryParserTest, *};

#[test]
fn test_subquery_in_where() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded WHERE id IN (SELECT id FROM other_table WHERE status = 'active')",
    )
    .into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_subquery_in_from() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM (SELECT id, email FROM sharded WHERE id = 1) AS sub",
    )
    .into()]);

    assert!(command.route().is_read());
}

#[test]
fn test_correlated_subquery() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded s1 WHERE EXISTS (SELECT 1 FROM sharded s2 WHERE s2.id = s1.id)",
    )
    .into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_scalar_subquery() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT *, (SELECT COUNT(*) FROM other_table) AS total FROM sharded WHERE id = 1",
    )
    .into()]);

    assert!(command.route().is_read());
}

#[test]
fn test_subquery_with_sharding_key() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named(
            "__test_sub",
            "SELECT * FROM sharded WHERE id = (SELECT id FROM other WHERE pk = $1)",
        )
        .into(),
        Bind::new_params("__test_sub", &[Parameter::new(b"5")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    // Can't route to specific shard because we don't know the subquery result
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_nested_subqueries() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded WHERE id IN (SELECT id FROM other WHERE status IN (SELECT status FROM statuses))",
    )
    .into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_cte_with_insert() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "WITH new_rows AS (INSERT INTO sharded (id, email) VALUES (1, 'test') RETURNING *) SELECT * FROM new_rows",
    )
    .into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_cte_with_delete() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "WITH deleted AS (DELETE FROM sharded WHERE id = 1 RETURNING *) SELECT * FROM deleted",
    )
    .into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_cte_with_update() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "WITH updated AS (UPDATE sharded SET email = 'new' WHERE id = 1 RETURNING *) SELECT * FROM updated",
    )
    .into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_recursive_cte() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "WITH RECURSIVE nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 10) SELECT * FROM nums",
    )
    .into()]);

    assert!(command.route().is_read());
}
