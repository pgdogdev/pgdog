use std::{collections::HashSet, str::from_utf8};

use rand::seq::SliceRandom;

use crate::{
    backend::server::test::test_server,
    config::{FlexibleType, ShardedMapping, ShardedMappingKind},
    net::{bind::Parameter, Bind, DataRow, Execute, FromBytes, Parse, Protocol, Query, Sync},
};

use super::*;

#[tokio::test]
async fn test_shard_varchar() {
    let mut words = ["apples", "oranges", "bananas", "dragon fruit", "peach"];

    let mut server = test_server().await;
    let inserts = (0..100)
        .map(|i| {
            words.shuffle(&mut rand::rng());
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

    assert_eq!(varchar(val) as usize % 3, expected_shard);

    let s = from_utf8(val).unwrap();
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
        .data(val)
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
                Bind::new_params_codes_results(
                    "",
                    &[Parameter {
                        len: 5,
                        data: "test1".as_bytes().into(),
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

    server.execute("ROLLBACK").await.unwrap();
}

#[tokio::test]
async fn test_shard_by_range() {
    let mut server = test_server().await;
    let inserts = (0..99)
        .map(|i| {
            Query::new(format!(
                "INSERT INTO test_shard_bigint_range (c) VALUES ({})",
                i
            ))
        })
        .collect::<Vec<_>>();
    let mut queries = vec![
        Query::new("BEGIN"),
        Query::new("CREATE TABLE test_shard_bigint_range (c BIGINT) PARTITION BY RANGE(c)"),
        Query::new("CREATE TABLE test_shard_bigint_range_0 PARTITION OF test_shard_bigint_range FOR VALUES FROM (0) TO (33)"),
        Query::new("CREATE TABLE test_shard_bigint_range_1 PARTITION OF test_shard_bigint_range FOR VALUES FROM (33) TO (66)"),
        Query::new("CREATE TABLE test_shard_bigint_range_2 PARTITION OF test_shard_bigint_range FOR VALUES FROM (66) TO (99)"),
    ];
    queries.extend(inserts);

    server.execute_batch(&queries).await.unwrap();

    let mut table = ShardedTable::default();
    table.data_type = DataType::Bigint;
    table.mapping = Mapping::new(
        &(0..3)
            .into_iter()
            .map(|s| ShardedMapping {
                kind: ShardedMappingKind::Range,
                start: Some(FlexibleType::Integer(s * 33)),
                end: Some(FlexibleType::Integer((s + 1) * 33)),
                shard: s as usize,
                ..Default::default()
            })
            .collect::<Vec<_>>(),
    );

    for shard in 0..3 {
        let table_name = format!("SELECT * FROM test_shard_bigint_range_{}", shard);
        let values = server.fetch_all::<i64>(&table_name).await.unwrap();
        for value in values {
            let context = ContextBuilder::new(&table)
                .data(value)
                .shards(3)
                .build()
                .unwrap();
            let calc = context.apply().unwrap();
            match calc {
                Shard::Direct(direct) => assert_eq!(direct, shard),
                _ => panic!("not a direct shard"),
            }
        }
    }

    server.execute("ROLLBACK").await.unwrap();
}

#[tokio::test]
async fn test_shard_by_list() {
    let mut server = test_server().await;
    let inserts = (0..30)
        .map(|i| {
            Query::new(format!(
                "INSERT INTO test_shard_bigint_list (c) VALUES ({})",
                i
            ))
        })
        .collect::<Vec<_>>();
    let mut queries = vec![
        Query::new("BEGIN"),
        Query::new("CREATE TABLE test_shard_bigint_list (c BIGINT) PARTITION BY LIST(c)"),
        Query::new("CREATE TABLE test_shard_bigint_list_0 PARTITION OF test_shard_bigint_list FOR VALUES IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)"),
        Query::new("CREATE TABLE test_shard_bigint_list_1 PARTITION OF test_shard_bigint_list FOR VALUES IN (10, 11, 12, 13, 14, 15, 16, 17, 18, 19)"),
        Query::new("CREATE TABLE test_shard_bigint_list_2 PARTITION OF test_shard_bigint_list FOR VALUES IN (20, 21, 22, 23, 24, 25, 26, 27, 28, 29)"),
    ];
    queries.extend(inserts);

    server.execute_batch(&queries).await.unwrap();

    let mut table = ShardedTable::default();
    table.data_type = DataType::Bigint;
    table.mapping = Mapping::new(
        &(0..3)
            .into_iter()
            .map(|s| ShardedMapping {
                kind: ShardedMappingKind::List,
                values: (s * 10..((s + 1) * 10))
                    .into_iter()
                    .map(|v| FlexibleType::Integer(v))
                    .collect::<HashSet<_>>(),
                shard: s as usize,
                ..Default::default()
            })
            .collect::<Vec<_>>(),
    );

    for shard in 0..3 {
        let table_name = format!("SELECT * FROM test_shard_bigint_list_{}", shard);
        let values = server.fetch_all::<i64>(&table_name).await.unwrap();
        for value in values {
            let context = ContextBuilder::new(&table)
                .data(value)
                .shards(3)
                .build()
                .unwrap();
            let calc = context.apply().unwrap();
            match calc {
                Shard::Direct(direct) => assert_eq!(direct, shard),
                _ => panic!("not a direct shard"),
            }
        }
    }

    server.execute("ROLLBACK").await.unwrap();
}

#[tokio::test]
async fn test_shard_by_uuid_list() {
    let mut server = test_server().await;

    let shard_0_uuids = vec![
        uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap(),
        uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
    ];

    let shard_1_uuids = vec![
        uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
        uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111112").unwrap(),
    ];

    let shard_2_uuids = vec![
        uuid::Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
        uuid::Uuid::parse_str("22222222-2222-2222-2222-222222222223").unwrap(),
    ];

    let uuid_sets = vec![
        shard_0_uuids.clone(),
        shard_1_uuids.clone(),
        shard_2_uuids.clone(),
    ];

    let mut insert_queries = Vec::new();
    for uuid in uuid_sets.clone().concat() {
        insert_queries.push(Query::new(format!(
            "INSERT INTO test_shard_uuid_list (id) VALUES ('{}')",
            uuid
        )));
    }

    let ddl_queries = vec![
        Query::new("CREATE TABLE test_shard_uuid_list (id UUID) PARTITION BY LIST(id)"),
        Query::new(&format!(
            "CREATE TABLE test_shard_uuid_list_0 PARTITION OF test_shard_uuid_list FOR VALUES IN ({})",
            shard_0_uuids.iter().map(|u| format!("'{}'", u)).collect::<Vec<_>>().join(", ")
        )),
        Query::new(&format!(
            "CREATE TABLE test_shard_uuid_list_1 PARTITION OF test_shard_uuid_list FOR VALUES IN ({})",
            shard_1_uuids.iter().map(|u| format!("'{}'", u)).collect::<Vec<_>>().join(", ")
        )),
        Query::new(&format!(
            "CREATE TABLE test_shard_uuid_list_2 PARTITION OF test_shard_uuid_list FOR VALUES IN ({})",
            shard_2_uuids.iter().map(|u| format!("'{}'", u)).collect::<Vec<_>>().join(", ")
        )),
    ];

    server.execute("BEGIN").await.unwrap();

    server.execute_batch(&ddl_queries).await.unwrap();
    server.execute_batch(&insert_queries).await.unwrap();

    let mut table = ShardedTable::default();
    table.data_type = DataType::Uuid;
    table.mapping = Mapping::new(
        &uuid_sets
            .into_iter()
            .enumerate()
            .map(|(shard, uuids)| ShardedMapping {
                kind: ShardedMappingKind::List,
                values: uuids
                    .into_iter()
                    .map(FlexibleType::Uuid)
                    .collect::<HashSet<_>>(),
                shard,
                ..Default::default()
            })
            .collect::<Vec<_>>(),
    );

    for shard in 0..3 {
        let query = format!("SELECT * FROM test_shard_uuid_list_{}", shard);
        let values = server.fetch_all::<String>(&query).await.unwrap();

        for value in values {
            let value = value.parse::<String>().unwrap();
            let context = ContextBuilder::new(&table)
                .data(value.as_str())
                .shards(3)
                .build()
                .unwrap();
            let calc = context.apply().unwrap();
            match calc {
                Shard::Direct(direct) => assert_eq!(direct, shard),
                _ => panic!("not a direct shard"),
            }
        }
    }

    server.execute("ROLLBACK").await.unwrap();
}

#[tokio::test]
async fn test_shard_by_list_with_default() {
    let mut server = test_server().await;

    // Create a table with list partitions for specific values and a default partition
    let queries = vec![
        Query::new("BEGIN"),
        Query::new("CREATE TABLE test_shard_list_default (c BIGINT) PARTITION BY LIST(c)"),
        Query::new("CREATE TABLE test_shard_list_default_0 PARTITION OF test_shard_list_default FOR VALUES IN (0, 1, 2)"),
        Query::new("CREATE TABLE test_shard_list_default_1 PARTITION OF test_shard_list_default FOR VALUES IN (10, 11, 12)"),
        Query::new("CREATE TABLE test_shard_list_default_2 PARTITION OF test_shard_list_default DEFAULT"),
    ];
    server.execute_batch(&queries).await.unwrap();

    // Insert values: some mapped explicitly, some going to default
    let inserts: Vec<Query> = [0, 1, 2, 10, 11, 12, 99, 100, 999]
        .iter()
        .map(|v| {
            Query::new(format!(
                "INSERT INTO test_shard_list_default (c) VALUES ({})",
                v
            ))
        })
        .collect();
    server.execute_batch(&inserts).await.unwrap();

    // Create mapping with explicit lists and a default shard
    let mut table = ShardedTable::default();
    table.data_type = DataType::Bigint;
    table.mapping = Mapping::new(&vec![
        ShardedMapping {
            kind: ShardedMappingKind::List,
            values: [0, 1, 2].into_iter().map(FlexibleType::Integer).collect(),
            shard: 0,
            ..Default::default()
        },
        ShardedMapping {
            kind: ShardedMappingKind::List,
            values: [10, 11, 12]
                .into_iter()
                .map(FlexibleType::Integer)
                .collect(),
            shard: 1,
            ..Default::default()
        },
        ShardedMapping {
            kind: ShardedMappingKind::Default,
            shard: 2,
            ..Default::default()
        },
    ]);

    // Verify explicitly mapped values route correctly
    for (shard, values) in [(0, vec![0, 1, 2]), (1, vec![10, 11, 12])] {
        for value in values {
            let context = ContextBuilder::new(&table)
                .data(value)
                .shards(3)
                .build()
                .unwrap();
            assert_eq!(context.apply().unwrap(), Shard::Direct(shard));
        }
    }

    // Verify unmapped values route to default shard
    for value in [99, 100, 999, -1, i64::MAX] {
        let context = ContextBuilder::new(&table)
            .data(value)
            .shards(3)
            .build()
            .unwrap();
        assert_eq!(context.apply().unwrap(), Shard::Direct(2));
    }

    // Verify data actually ended up in the right partitions
    let shard_0_values = server
        .fetch_all::<i64>("SELECT * FROM test_shard_list_default_0")
        .await
        .unwrap();
    assert_eq!(
        shard_0_values.iter().collect::<HashSet<_>>(),
        [0, 1, 2].iter().collect()
    );

    let shard_1_values = server
        .fetch_all::<i64>("SELECT * FROM test_shard_list_default_1")
        .await
        .unwrap();
    assert_eq!(
        shard_1_values.iter().collect::<HashSet<_>>(),
        [10, 11, 12].iter().collect()
    );

    let shard_2_values = server
        .fetch_all::<i64>("SELECT * FROM test_shard_list_default_2")
        .await
        .unwrap();
    assert_eq!(
        shard_2_values.iter().collect::<HashSet<_>>(),
        [99, 100, 999].iter().collect()
    );

    server.execute("ROLLBACK").await.unwrap();
}
