use crate::{backend::pool::Request, frontend::router::parser::Shard};

use super::setup::*;

#[test]
fn test_comment_pgdog_lsn() {
    let mut test = QueryParserTest::new();

    for (sql, expected) in [
        ("SELECT 1", None),
        ("/* pgdog_lsn: 0/16B6C50 */ SELECT 1", Some(0x16B6C50)),
        ("/* pgdog_lsn: 1/0 */ SELECT 1", Some(1 << 32)),
        ("/* pgdog_lsn: 80000000/0 */ SELECT 1", Some(1 << 63)),
        (
            "/* pgdog_lsn: FFFFFFFF/FFFFFFFF */ SELECT 1",
            Some(u64::MAX),
        ),
    ] {
        let command = test.execute(vec![Query::new(sql).into()]);
        let route = command.route();
        assert_eq!(route.lsn, expected, "{sql}");
        let request = Request::new(Default::default(), route, false);
        assert_eq!(request.lsn, expected, "{sql}");
    }
}

#[test]
fn test_comment_pgdog_role_primary() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("/* pgdog_role: primary */ SELECT 1").into(),
    ]);

    assert!(command.route().is_write());
}

#[test]
fn test_comment_pgdog_shard() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("/* pgdog_shard: 1234 */ SELECT 1234").into(),
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
