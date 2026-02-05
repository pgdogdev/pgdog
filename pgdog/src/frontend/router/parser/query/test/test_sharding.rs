use std::collections::HashSet;
use std::ops::Deref;

use crate::config::config;
use crate::frontend::router::parser::{Cache, Shard};
use crate::frontend::Command;

use super::setup::{QueryParserTest, *};

use pgdog_config::ShardedTable;

#[test]
fn test_show_shards() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SHOW pgdog.shards").into()]);

    assert!(matches!(command, Command::InternalField { .. }));
}

#[test]
fn test_close_direct_single_shard() {
    let mut test = QueryParserTest::new_single_shard(&config());

    let command = test.execute(vec![Close::named("test").into(), Sync.into()]);

    match command {
        Command::Query(route) => assert_eq!(route.shard(), &Shard::Direct(0), "{:?}", route),
        _ => panic!("expected Query, got {command:?}"),
    }
}

#[test]
fn test_dry_run_simple() {
    let mut test = QueryParserTest::new_single_shard(&config()).with_dry_run();

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
    let q = "SELECT * FROM sharded_omni INNER JOIN not_sharded ON sharded_omni.id = not_sharded.id INNER JOIN sharded ON sharded.id = sharded_omni.id WHERE sharded_omni = $1";

    let mut test = QueryParserTest::new();
    let command = test.execute(vec![Query::new(q).into()]);

    assert!(matches!(command.route().shard(), Shard::All));
}

#[test]
fn test_omni_flag_set_for_select() {
    let mut test = QueryParserTest::new();
    let q = "SELECT * FROM sharded_omni WHERE id = 1";
    let command = test.execute(vec![Query::new(q).into()]);
    assert!(command.route().is_omni());
}

#[test]
fn test_omni_flag_set_for_update() {
    let mut test = QueryParserTest::new();
    let q = "UPDATE sharded_omni SET value = 'test' WHERE id = 1";
    let command = test.execute(vec![Query::new(q).into()]);
    assert!(command.route().is_omni());
}

#[test]
fn test_omni_flag_set_for_delete() {
    let mut test = QueryParserTest::new();
    let q = "DELETE FROM sharded_omni WHERE id = 1";
    let command = test.execute(vec![Query::new(q).into()]);
    assert!(command.route().is_omni());
}

#[test]
fn test_omni_flag_set_for_insert() {
    let mut test = QueryParserTest::new();
    let q = "INSERT INTO sharded_omni (id, value) VALUES (1, 'test')";
    let command = test.execute(vec![Query::new(q).into()]);
    assert!(command.route().is_omni());
}

#[test]
fn test_omni_flag_not_set_for_regular_sharded() {
    let mut test = QueryParserTest::new();
    let q = "SELECT * FROM sharded WHERE id = 1";
    let command = test.execute(vec![Query::new(q).into()]);
    assert!(!command.route().is_omni());
}

#[test]
fn test_omni_flag_not_set_when_joined_with_sharded() {
    let mut test = QueryParserTest::new();
    let q = "SELECT * FROM sharded_omni INNER JOIN sharded ON sharded_omni.id = sharded.id WHERE sharded.id = 5";
    let command = test.execute(vec![Query::new(q).into()]);
    assert!(!command.route().is_omni());
}

/// Test that omnisharded config overrides sharded table config.
/// When a table is in both sharded_tables AND omnisharded config,
/// the omnisharded config should take priority.
///
/// Note: Cluster::new_test() hardcodes omnisharded tables (sharded_omni, sharded_omni_sticky)
/// so we test by adding "sharded_omni" to sharded_tables and verifying omnisharded wins.
#[test]
fn test_omnisharded_overrides_sharded_table_config() {
    // Add "sharded_omni" (which is already in omnisharded config in Cluster::new_test)
    // to sharded_tables config - omnisharded should still take priority
    let mut config_with_both = config().deref().clone();
    config_with_both.config.sharded_tables.push(ShardedTable {
        database: "pgdog".into(),
        name: Some("sharded_omni".into()),
        column: "id".into(),
        ..Default::default()
    });

    // Query against "sharded_omni" which is now in BOTH sharded_tables AND omnisharded
    // Should be treated as omnisharded (round-robin), NOT sharded (deterministic)
    let q = "SELECT * FROM sharded_omni WHERE id = 1";

    // Run multiple times to verify round-robin behavior (omnisharded)
    let mut shards_seen = HashSet::new();
    for _ in 0..10 {
        let mut test = QueryParserTest::new_with_config(&config_with_both);
        let command = test.execute(vec![Query::new(q).into()]);
        match command {
            Command::Query(route) => {
                assert!(
                    route.is_omni(),
                    "Query against table in both configs should have omni flag (omnisharded wins)"
                );
                shards_seen.insert(route.shard().clone());
            }
            _ => panic!("Expected Query command"),
        }
    }

    // Should see multiple shards due to round-robin (omnisharded behavior)
    // If sharded config won, we'd see only one shard (deterministic)
    assert!(
        shards_seen.len() > 1,
        "Omnisharded should override sharded config, using round-robin. Saw {:?}",
        shards_seen
    );
}
