use crate::frontend::router::parser::Shard;
use crate::net::messages::Parameter;

use super::setup::*;

#[test]
fn test_insert_with_params() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named(
            "__test_insert",
            "INSERT INTO sharded (id, email) VALUES ($1, $2)",
        )
        .into(),
        Bind::new_params(
            "__test_insert",
            &[Parameter::new(b"11"), Parameter::new(b"test@test.com")],
        )
        .into(),
        Describe::new_statement("__test_insert").into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_insert_returning() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "INSERT INTO foo (id) VALUES ($1::UUID) ON CONFLICT (id) DO UPDATE SET id = excluded.id RETURNING id",
    )
    .into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_select_for_update() {
    let mut test = QueryParserTest::new();

    // Without parameters - goes to all shards
    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded WHERE id = $1 FOR UPDATE",
    )
    .into()]);
    assert!(command.route().is_write());
    assert!(matches!(command.route().shard(), Shard::All));

    // With parameter - goes to specific shard
    let mut test = QueryParserTest::new();
    let command = test.execute(vec![
        Parse::named(
            "__test_sfu",
            "SELECT * FROM sharded WHERE id = $1 FOR UPDATE",
        )
        .into(),
        Bind::new_params("__test_sfu", &[Parameter::new(b"1")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
    assert!(command.route().is_write());
}
