use std::collections::HashSet;

use crate::frontend::router::parser::{Cache, Shard};
use crate::frontend::Command;

use super::setup::{QueryParserTest, *};

#[test]
fn test_show_shards() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SHOW pgdog.shards").into()]);

    assert!(matches!(command, Command::InternalField { .. }));
}

#[test]
fn test_close_direct_single_shard() {
    let mut test = QueryParserTest::new_single_shard();

    let command = test.execute(vec![Close::named("test").into(), Sync.into()]);

    match command {
        Command::Query(route) => assert_eq!(route.shard(), &Shard::Direct(0), "{:?}", route),
        _ => panic!("expected Query, got {command:?}"),
    }
}

#[test]
fn test_dry_run_simple() {
    let mut test = QueryParserTest::new_single_shard().with_dry_run();

    let command = test.execute(vec![Query::new(
        "/* pgdog_sharding_key: 1234 */ SELECT * FROM sharded",
    )
    .into()]);

    let cache = Cache::queries();
    let stmt = cache.values().next().unwrap();
    assert_eq!(stmt.stats.lock().direct, 1);
    assert_eq!(stmt.stats.lock().multi, 0);
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_omni_round_robin() {
    let mut omni_round_robin = HashSet::new();
    let q = "SELECT sharded_omni.* FROM sharded_omni WHERE sharded_omni.id = 1";

    for _ in 0..10 {
        let mut test = QueryParserTest::new();
        let command = test.execute(vec![Query::new(q).into()]);

        match command {
            Command::Query(query) => {
                assert!(matches!(query.shard(), Shard::Direct(_)));
                omni_round_robin.insert(query.shard().clone());
            }
            _ => {}
        }
    }

    assert_eq!(omni_round_robin.len(), 2);
}

#[test]
fn test_omni_sticky() {
    let mut omni_sticky = HashSet::new();
    let q =
        "SELECT sharded_omni_sticky.* FROM sharded_omni_sticky WHERE sharded_omni_sticky.id = $1";

    // Use a single test instance with consistent Sticky across all iterations
    let mut test = QueryParserTest::new();
    for _ in 0..10 {
        let command = test.execute(vec![Query::new(q).into()]);

        match command {
            Command::Query(query) => {
                assert!(matches!(query.shard(), Shard::Direct(_)));
                omni_sticky.insert(query.shard().clone());
            }
            _ => {}
        }
    }

    assert_eq!(omni_sticky.len(), 1);
}

#[test]
fn test_omni_sharded_table_takes_priority() {
    let q = "
        SELECT
            sharded_omni.*,
            sharded.*
        FROM
            sharded_omni
        INNER JOIN
            sharded
        ON sharded_omni.id = sharded.i
    WHERE sharded.id = 5";

    let mut test = QueryParserTest::new();
    let command = test.execute(vec![Query::new(q).into()]);
    let first_shard = command.route().shard().clone();

    for _ in 0..5 {
        let mut test = QueryParserTest::new();
        let command = test.execute(vec![Query::new(q).into()]);
        assert_eq!(&first_shard, command.route().shard());
        assert!(matches!(first_shard, Shard::Direct(_)));
    }
}

#[test]
fn test_omni_all_tables_must_be_omnisharded() {
    let q = "SELECT * FROM sharded_omni INNER JOIN not_sharded ON sharded_omni.id = not_sharded.id WHERE sharded_omni = $1";

    let mut test = QueryParserTest::new();
    let command = test.execute(vec![Query::new(q).into()]);

    assert!(matches!(command.route().shard(), Shard::All));
}
