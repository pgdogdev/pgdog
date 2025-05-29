use std::str::from_utf8;

use rand::{seq::SliceRandom, thread_rng};

use crate::{
    backend::server::test::test_server,
    net::{bind::Parameter, Bind, DataRow, Execute, FromBytes, Parse, Protocol, Query, Sync},
};

use super::*;

#[tokio::test]
async fn test_shard_varchar() {
    let mut words = ["apples", "oranges", "bananas", "dragon fruit", "peach"];

    let mut server = test_server().await;
    let inserts = (0..100)
        .into_iter()
        .map(|i| {
            words.shuffle(&mut thread_rng());
            let word = words.first().unwrap();

            Query::new(format!(
                "INSERT INTO test_shard_varchar (c) VALUES ('{}')",
                format!("{}_{}_{}", i, word, i)
            ))
        })
        .collect::<Vec<_>>();
    let mut queries = vec![
        Query::new("BEGIN"),
        Query::new("CREATE TABLE test_shard_varchar (c VARCHAR) PARTITION BY HASH(c)"),
        Query::new("CREATE TABLE test_shard_varchar_0 PARTITION OF test_shard_varchar FOR VALUES WITH (modulus 3, remainder 0)"),
        Query::new("CREATE TABLE test_shard_varchar_1 PARTITION OF test_shard_varchar FOR VALUES WITH (modulus 3, remainder 1)"),
        Query::new("CREATE TABLE test_shard_varchar_2 PARTITION OF test_shard_varchar FOR VALUES WITH (modulus 3, remainder 2)"),
    ];
    queries.extend(inserts);

    server.execute_batch(&queries).await.unwrap();

    let mut schema = ShardingSchema::default();
    schema.shards = 3;

    let mut table = ShardedTable::default();
    table.data_type = DataType::Varchar;

    let shard_0 = server
        .execute("SELECT * FROM test_shard_varchar_0")
        .await
        .unwrap()
        .into_iter()
        .filter(|m| m.code() == 'D')
        .map(|d| DataRow::from_bytes(d.payload()).unwrap().column(0).unwrap())
        .collect::<Vec<_>>();
    assert!(!shard_0.is_empty());
    for val in &shard_0 {
        assert_shard(val, 0);
    }

    let shard_1 = server
        .execute("SELECT * FROM test_shard_varchar_1")
        .await
        .unwrap()
        .into_iter()
        .filter(|m| m.code() == 'D')
        .map(|d| DataRow::from_bytes(d.payload()).unwrap().column(0).unwrap())
        .collect::<Vec<_>>();
    assert!(!shard_1.is_empty());
    for val in &shard_1 {
        assert_shard(val, 1);
    }
    let shard_2 = server
        .execute("SELECT * FROM test_shard_varchar_2")
        .await
        .unwrap()
        .into_iter()
        .filter(|m| m.code() == 'D')
        .map(|d| DataRow::from_bytes(d.payload()).unwrap().column(0).unwrap())
        .collect::<Vec<_>>();
    assert!(!shard_2.is_empty());
    for val in &shard_2 {
        assert_shard(val, 2);
    }
    server.execute("ROLLBACK").await.unwrap();
}

fn assert_shard(val: &[u8], expected_shard: usize) {
    let mut schema = ShardingSchema::default();
    schema.shards = 3;

    let mut table = ShardedTable::default();
    table.data_type = DataType::Varchar;

    assert_eq!(varchar(&val[..]).unwrap() as usize % 3, expected_shard);

    let s = from_utf8(&val[..]).unwrap();
    let shard = shard_str(s, &schema, &vec![], 0);
    assert_eq!(shard, Shard::Direct(expected_shard));
    let shard = shard_value(
        s,
        &crate::config::DataType::Varchar,
        3,
        &vec![],
        expected_shard,
    );
    assert_eq!(shard, Shard::Direct(expected_shard));
    let ctx = ContextBuilder::new(&table)
        .data(&val[..])
        .shards(3)
        .build()
        .unwrap();
    let shard = ctx.apply().unwrap();
    assert_eq!(shard, Shard::Direct(expected_shard));
}

#[tokio::test]
async fn test_binary_encoding() {
    let mut server = test_server().await;

    server
        .send(
            &vec![
                Parse::new_anonymous("SELECT $1::varchar").into(),
                Bind::test_params_codes_results(
                    "",
                    &[Parameter {
                        len: 5,
                        data: "test1".as_bytes().to_vec(),
                    }],
                    &[Format::Binary],
                    &[1],
                )
                .into(),
                Execute::new().into(),
                Sync.into(),
            ]
            .into(),
        )
        .await
        .unwrap();

    for c in ['1', '2', 'D', 'C', 'Z'] {
        let msg = server.read().await.unwrap();
        if c == 'D' {
            let dr = DataRow::from_bytes(msg.payload()).unwrap();
            assert_eq!(dr.column(0).unwrap(), "test1".as_bytes()); // Binary encoding is just UTF-8, no null terminator.
        }
        assert!(msg.code() == c);
    }
}
