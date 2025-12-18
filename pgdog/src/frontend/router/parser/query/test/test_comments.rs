use crate::frontend::router::parser::Shard;

use super::setup::*;

#[test]
fn test_comment_pgdog_role_primary() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("/* pgdog_role: primary */ SELECT 1").into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_comment_pgdog_shard() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("/* pgdog_shard: 1234 */ SELECT 1234").into()
    ]);

    assert_eq!(command.route().shard(), &Shard::Direct(1234));
}

#[test]
fn test_comment_pgdog_shard_extended() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named(
            "__test_comment",
            "/* pgdog_shard: 1234 */ SELECT * FROM sharded WHERE id = $1",
        )
        .into(),
        Bind::new_statement("__test_comment").into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert_eq!(command.route().shard(), &Shard::Direct(1234));
}
