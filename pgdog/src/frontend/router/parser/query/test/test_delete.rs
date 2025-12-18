use crate::frontend::router::parser::Shard;
use crate::net::messages::Parameter;

use super::setup::{QueryParserTest, *};

#[test]
fn test_delete_with_sharding_key() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named("__test_delete", "DELETE FROM sharded WHERE id = $1").into(),
        Bind::new_params("__test_delete", &[Parameter::new(b"5")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert!(command.route().is_write());
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

#[test]
fn test_delete_without_sharding_key() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("DELETE FROM sharded WHERE email = 'test'").into()
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_delete_all_rows() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("DELETE FROM sharded").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_delete_with_returning() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named(
            "__test_del_ret",
            "DELETE FROM sharded WHERE id = $1 RETURNING *",
        )
        .into(),
        Bind::new_params("__test_del_ret", &[Parameter::new(b"7")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert!(command.route().is_write());
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

#[test]
fn test_delete_with_subquery() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "DELETE FROM sharded WHERE id IN (SELECT id FROM other_table WHERE status = 'inactive')",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_delete_using_join() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "DELETE FROM sharded USING other_table WHERE sharded.id = other_table.sharded_id",
    )
    .into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::All);
}
