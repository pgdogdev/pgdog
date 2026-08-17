use std::collections::HashSet;

use crate::frontend::router::parser::Shard;

use super::setup::*;

#[test]
fn test_prepared() {
    let mut test = QueryParserTest::new().with_full_prepared_statements();

    let command = test.execute(vec![
        Query::new("PREPARE __stmt_1 AS SELECT * FROM sharded WHERE id = $1").into(),
    ]);
    assert!(command.route().is_read());
    assert!(matches!(command.route().shard(), Shard::Direct(_)));

    let command = test.execute(vec![Query::new("EXECUTE __stmt_1(11)").into()]);
    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_prepared_omnisharded_table() {
    let mut test = QueryParserTest::new().with_full_prepared_statements();

    test.execute(vec![
        Query::new("PREPARE __stmt_1 AS SELECT * FROM sharded_omni WHERE id = $1").into(),
    ]);

    let command = test.execute(vec![Query::new("EXECUTE __stmt_1(11)").into()]);
    assert!(command.route().is_read());
    assert!(command.route().is_omnisharded());
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

#[test]
fn test_prepare_uses_round_robin_across_calls() {
    let mut test = QueryParserTest::new();
    let mut shards = HashSet::new();

    for name in ["stmt_1", "stmt_2", "stmt_3", "stmt_4"] {
        let command = test.execute(vec![
            Query::new(format!("PREPARE {name} AS SELECT 1")).into(),
        ]);

        assert!(command.route().is_read());
        let Shard::Direct(shard) = command.route().shard() else {
            panic!("PREPARE should route to a direct shard");
        };
        shards.insert(*shard);
    }

    assert_eq!(shards.len(), 2);
}
