use crate::frontend::router::parser::Shard;

use super::setup::*;

fn copy_to_shard(query: &str) -> Shard {
    let mut test = QueryParserTest::new();
    let command = test.execute(vec![Query::new(query).into()]);
    command.route().shard().clone()
}

#[test]
fn test_copy_to_omnisharded_table_reads_one_shard() {
    let shard = copy_to_shard("COPY sharded_omni TO STDOUT");
    assert!(matches!(shard, Shard::Direct(_)), "{shard:?}");

    let shard = copy_to_shard("COPY sharded_omni_sticky TO STDOUT");
    assert!(matches!(shard, Shard::Direct(_)), "{shard:?}");
}

#[test]
fn test_copy_to_omnisharded_table_uses_sticky_shard() {
    let mut test = QueryParserTest::new();
    let shards: Vec<_> = (0..4)
        .map(|_| {
            test.execute(vec![Query::new("COPY sharded_omni TO STDOUT").into()])
                .route()
                .shard()
                .clone()
        })
        .collect();

    assert!(shards.iter().all(|shard| shard == &shards[0]), "{shards:?}");
}

#[test]
fn test_copy_to_sharded_table_reads_all_shards() {
    assert_eq!(copy_to_shard("COPY sharded TO STDOUT"), Shard::All);
}

#[test]
fn test_copy_to_unknown_table_reads_all_shards() {
    assert_eq!(copy_to_shard("COPY unknown_table TO STDOUT"), Shard::All);
}

#[test]
fn test_copy_query_to_reads_all_shards() {
    assert_eq!(
        copy_to_shard("COPY (SELECT * FROM sharded_omni) TO STDOUT"),
        Shard::All
    );
}
