use crate::frontend::router::parser::{
    route::{RoundRobinReason, ShardSource},
    Shard,
};

use super::setup::*;

#[test]
fn test_rr_executable() {
    let mut test = QueryParserTest::new();
    let command = test.execute(vec![
        Parse::named(
            "__test_1",
            "INSERT INTO some_table (id, value) VALUES ($1, $2)",
        )
        .into(),
        Describe::new_statement("__test_1").into(),
        Flush.into(),
    ]);

    assert!(matches!(command.route().shard(), Shard::Direct(_)));
    assert_eq!(
        test.parser.shard.peek().unwrap().source(),
        &ShardSource::RoundRobin(RoundRobinReason::NotExecutable)
    );

    let command = test.execute(vec![
        Bind::new_params("__test_1", &[]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert!(matches!(command.route().shard(), Shard::All));
}
