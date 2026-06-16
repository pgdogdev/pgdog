use std::{collections::HashSet, str::from_utf8};

use pgdog_config::ShardedTableConfig;
use rand::seq::SliceRandom;

use crate::config::{
    ShardedMappingConfig, ShardedMappingDeprecated, ShardedMappingKindDeprecated,
    ShardedMappingList, ShardedMappingRange,
};
use crate::{
    backend::server::test::test_server,
    config::FlexibleType,
    net::{Bind, DataRow, Execute, FromBytes, Parse, Protocol, Query, Sync, bind::Parameter},
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
                "INSERT INTO test_shard_varchar (c) VALUES ('{}_{}_{}')",
                i, word, i
            ))
        })
        .collect::<Vec<_>>();
    let mut queries = vec![
        Query::new("BEGIN"),
        Query::new("CREATE TABLE test_shard_varchar (c VARCHAR) PARTITION BY HASH(c)"),
        Query::new(
            "CREATE TABLE test_shard_varchar_0 PARTITION OF test_shard_varchar FOR VALUES WITH (modulus 3, remainder 0)",
        ),
        Query::new(
            "CREATE TABLE test_shard_varchar_1 PARTITION OF test_shard_varchar FOR VALUES WITH (modulus 3, remainder 1)",
        ),
        Query::new(
            "CREATE TABLE test_shard_varchar_2 PARTITION OF test_shard_varchar FOR VALUES WITH (modulus 3, remainder 2)",
        ),
    ];
    queries.extend(inserts);

    server.execute_batch(&queries).await.unwrap();

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
    let schema = ShardingSchema {
        shards: 3,
        ..Default::default()
    };

    let table = ShardedTable {
        data_type: DataType::Varchar,
        ..Default::default()
    };

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
        Query::new(
            "CREATE TABLE test_shard_bigint_range_0 PARTITION OF test_shard_bigint_range FOR VALUES FROM (0) TO (33)",
        ),
        Query::new(
            "CREATE TABLE test_shard_bigint_range_1 PARTITION OF test_shard_bigint_range FOR VALUES FROM (33) TO (66)",
        ),
        Query::new(
            "CREATE TABLE test_shard_bigint_range_2 PARTITION OF test_shard_bigint_range FOR VALUES FROM (66) TO (99)",
        ),
    ];
    queries.extend(inserts);

    server.execute_batch(&queries).await.unwrap();

    let table = ShardedTable {
        data_type: DataType::Bigint,
        mapping: Mapping::new(
            (0..3)
                .map(|s| {
                    ShardedMappingConfig::Range(ShardedMappingRange {
                        start: Some(FlexibleType::Integer(s * 33)),
                        end: Some(FlexibleType::Integer((s + 1) * 33)),
                        shard: s as usize,
                    })
                })
                .collect::<Vec<_>>(),
        ),
        ..Default::default()
    };

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
        Query::new(
            "CREATE TABLE test_shard_bigint_list_0 PARTITION OF test_shard_bigint_list FOR VALUES IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)",
        ),
        Query::new(
            "CREATE TABLE test_shard_bigint_list_1 PARTITION OF test_shard_bigint_list FOR VALUES IN (10, 11, 12, 13, 14, 15, 16, 17, 18, 19)",
        ),
        Query::new(
            "CREATE TABLE test_shard_bigint_list_2 PARTITION OF test_shard_bigint_list FOR VALUES IN (20, 21, 22, 23, 24, 25, 26, 27, 28, 29)",
        ),
    ];
    queries.extend(inserts);

    server.execute_batch(&queries).await.unwrap();

    let table = ShardedTable {
        data_type: DataType::Bigint,
        mapping: Mapping::new(
            (0..3)
                .map(|s| {
                    ShardedMappingConfig::List(ShardedMappingList {
                        values: (s * 10..((s + 1) * 10))
                            .map(FlexibleType::Integer)
                            .collect::<Vec<_>>(),
                        shard: s as usize,
                    })
                })
                .collect::<Vec<_>>(),
        ),
        ..Default::default()
    };

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
        Query::new(format!(
            "CREATE TABLE test_shard_uuid_list_0 PARTITION OF test_shard_uuid_list FOR VALUES IN ({})",
            shard_0_uuids
                .iter()
                .map(|u| format!("'{}'", u))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        Query::new(format!(
            "CREATE TABLE test_shard_uuid_list_1 PARTITION OF test_shard_uuid_list FOR VALUES IN ({})",
            shard_1_uuids
                .iter()
                .map(|u| format!("'{}'", u))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        Query::new(format!(
            "CREATE TABLE test_shard_uuid_list_2 PARTITION OF test_shard_uuid_list FOR VALUES IN ({})",
            shard_2_uuids
                .iter()
                .map(|u| format!("'{}'", u))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    ];

    server.execute("BEGIN").await.unwrap();

    server.execute_batch(&ddl_queries).await.unwrap();
    server.execute_batch(&insert_queries).await.unwrap();

    let table = ShardedTable {
        data_type: DataType::Uuid,
        mapping: Mapping::new(
            uuid_sets
                .into_iter()
                .enumerate()
                .map(|(shard, uuids)| {
                    ShardedMappingConfig::List(ShardedMappingList {
                        values: uuids
                            .into_iter()
                            .map(FlexibleType::Uuid)
                            .collect::<Vec<_>>(),
                        shard,
                    })
                })
                .collect::<Vec<_>>(),
        ),
        ..Default::default()
    };

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
        Query::new(
            "CREATE TABLE test_shard_list_default_0 PARTITION OF test_shard_list_default FOR VALUES IN (0, 1, 2)",
        ),
        Query::new(
            "CREATE TABLE test_shard_list_default_1 PARTITION OF test_shard_list_default FOR VALUES IN (10, 11, 12)",
        ),
        Query::new(
            "CREATE TABLE test_shard_list_default_2 PARTITION OF test_shard_list_default DEFAULT",
        ),
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
    let table = ShardedTable {
        data_type: DataType::Bigint,
        mapping: Mapping::new(vec![
            ShardedMappingConfig::List(ShardedMappingList {
                values: [0, 1, 2].into_iter().map(FlexibleType::Integer).collect(),
                shard: 0,
            }),
            ShardedMappingConfig::List(ShardedMappingList {
                values: [10, 11, 12]
                    .into_iter()
                    .map(FlexibleType::Integer)
                    .collect(),
                shard: 1,
            }),
            ShardedMappingConfig::Default { shard: 2 },
        ]),
        ..Default::default()
    };

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

fn make_sharding_schema(
    sharded_tables: Vec<ShardedTableConfig>,
    sharded_mappings: Vec<ShardedMappingDeprecated>,
) -> ShardingSchema {
    use crate::backend::databases::from_config;
    use crate::config::{Config, ConfigAndUsers, Database, Role, User, Users};
    use std::path::PathBuf;

    let config = Config {
        databases: vec![
            Database {
                name: "db".into(),
                host: "localhost".into(),
                port: 5432,
                role: Role::Primary,
                shard: 0,
                ..Default::default()
            },
            Database {
                name: "db".into(),
                host: "localhost".into(),
                port: 5432,
                role: Role::Primary,
                shard: 1,
                ..Default::default()
            },
        ],
        sharded_tables,
        sharded_mappings,
        ..Default::default()
    };
    let users = Users {
        users: vec![User {
            name: "user".into(),
            database: "db".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let databases = from_config(&ConfigAndUsers {
        config,
        users,
        config_path: PathBuf::new(),
        users_path: PathBuf::new(),
    });
    databases.cluster(("user", "db")).unwrap().sharding_schema()
}

fn route(schema: &ShardingSchema, table_name: &str, value: i64) -> Shard {
    use crate::frontend::router::parser::Column;

    let col = Column {
        name: "id",
        table: Some(table_name),
        schema: None,
    };

    let t = schema
        .tables()
        .get_table(col)
        .expect("table/column not found in schema");

    ContextBuilder::new(t)
        .data(value)
        .shards(schema.shards)
        .build()
        .unwrap()
        .apply()
        .unwrap()
}

/// Tests for the deprecated top-level `sharded_mappings` config format.
///
/// This whole module can be dropped when `ShardedMappingDeprecated` support is
/// removed.
mod deprecated_mappings {
    use super::*;

    #[test]
    fn test_same_tables_specific_and_mapping_specific() {
        let schema = make_sharding_schema(
            vec![ShardedTableConfig {
                database: "db".into(),
                name: Some("users".into()),
                column: "id".into(),
                data_type: DataType::Bigint,
                ..Default::default()
            }],
            vec![
                ShardedMappingDeprecated {
                    database: "db".into(),
                    column: "id".into(),
                    table: Some("users".into()),
                    kind: ShardedMappingKindDeprecated::List,
                    values: [1, 2, 3].into_iter().map(FlexibleType::Integer).collect(),
                    shard: 0,
                    ..Default::default()
                },
                ShardedMappingDeprecated {
                    database: "db".into(),
                    column: "id".into(),
                    table: Some("users".into()),
                    kind: ShardedMappingKindDeprecated::List,
                    values: [4, 5, 6].into_iter().map(FlexibleType::Integer).collect(),
                    shard: 1,
                    ..Default::default()
                },
            ],
        );

        for v in [1, 2, 3] {
            assert_eq!(
                route(&schema, "users", v),
                Shard::Direct(0),
                "id={v} should go to shard 0"
            );
        }
        for v in [4, 5, 6] {
            assert_eq!(
                route(&schema, "users", v),
                Shard::Direct(1),
                "id={v} should go to shard 1"
            );
        }
    }

    #[test]
    fn test_different_tables_specific_and_mapping_specific() {
        let schema = make_sharding_schema(
            vec![ShardedTableConfig {
                database: "db".into(),
                name: Some("users".into()),
                column: "id".into(),
                data_type: DataType::Bigint,
                ..Default::default()
            }],
            vec![ShardedMappingDeprecated {
                database: "db".into(),
                column: "id".into(),
                table: Some("orders".into()),
                kind: ShardedMappingKindDeprecated::List,
                values: [1, 2, 3].into_iter().map(FlexibleType::Integer).collect(),
                shard: 0,
                ..Default::default()
            }],
        );

        for v in [1i64, 2, 3, 4, 5, 6] {
            assert_eq!(
                route(&schema, "users", v),
                Shard::Direct(bigint(v) as usize % 2),
                "id={v}"
            );
        }
    }

    #[test]
    fn test_tables_all_and_mapping_all() {
        let schema = make_sharding_schema(
            vec![ShardedTableConfig {
                database: "db".into(),
                column: "id".into(),
                data_type: DataType::Bigint,
                ..Default::default()
            }],
            vec![
                ShardedMappingDeprecated {
                    database: "db".into(),
                    column: "id".into(),
                    kind: ShardedMappingKindDeprecated::List,
                    values: [1, 2, 3].into_iter().map(FlexibleType::Integer).collect(),
                    shard: 0,
                    ..Default::default()
                },
                ShardedMappingDeprecated {
                    database: "db".into(),
                    column: "id".into(),
                    kind: ShardedMappingKindDeprecated::List,
                    values: [4, 5, 6].into_iter().map(FlexibleType::Integer).collect(),
                    shard: 1,
                    ..Default::default()
                },
            ],
        );

        for i in [1, 2, 3] {
            assert_eq!(route(&schema, "users", i), Shard::Direct(0));
            assert_eq!(route(&schema, "orders", i), Shard::Direct(0));
        }

        for i in [4, 5, 6] {
            assert_eq!(route(&schema, "users", i), Shard::Direct(1));
            assert_eq!(route(&schema, "orders", i), Shard::Direct(1));
        }
    }

    #[test]
    fn test_tables_specific_and_mapping_all() {
        let schema = make_sharding_schema(
            vec![ShardedTableConfig {
                database: "db".into(),
                name: Some("users".into()),
                column: "id".into(),
                data_type: DataType::Bigint,
                ..Default::default()
            }],
            vec![
                ShardedMappingDeprecated {
                    database: "db".into(),
                    column: "id".into(),
                    kind: ShardedMappingKindDeprecated::List,
                    values: [1, 2, 3].into_iter().map(FlexibleType::Integer).collect(),
                    shard: 0,
                    ..Default::default()
                },
                ShardedMappingDeprecated {
                    database: "db".into(),
                    column: "id".into(),
                    kind: ShardedMappingKindDeprecated::List,
                    values: [4, 5, 6].into_iter().map(FlexibleType::Integer).collect(),
                    shard: 1,
                    ..Default::default()
                },
            ],
        );

        assert!(
            schema
                .tables()
                .tables()
                .iter()
                .next()
                .unwrap()
                .mapping
                .is_none(),
            "column-wide mapping must not apply to a named table"
        );

        for v in [1i64, 2, 3, 4, 5, 6] {
            assert_eq!(
                route(&schema, "users", v),
                Shard::Direct(bigint(v) as usize % 2),
                "id={v}"
            );
        }
    }

    #[test]
    fn test_tables_all_and_mapping_specific() {
        let schema = make_sharding_schema(
            vec![ShardedTableConfig {
                database: "db".into(),
                column: "id".into(),
                data_type: DataType::Bigint,
                ..Default::default()
            }],
            vec![
                ShardedMappingDeprecated {
                    database: "db".into(),
                    table: Some("users".into()),
                    column: "id".into(),
                    kind: ShardedMappingKindDeprecated::List,
                    values: [1, 2, 3].into_iter().map(FlexibleType::Integer).collect(),
                    shard: 0,
                    ..Default::default()
                },
                ShardedMappingDeprecated {
                    database: "db".into(),
                    table: Some("users".into()),
                    column: "id".into(),
                    kind: ShardedMappingKindDeprecated::List,
                    values: [4, 5, 6].into_iter().map(FlexibleType::Integer).collect(),
                    shard: 1,
                    ..Default::default()
                },
            ],
        );

        assert!(
            schema
                .tables()
                .tables()
                .iter()
                .next()
                .unwrap()
                .mapping
                .is_none(),
            "table-specific mapping must not apply to a column-wide table"
        );

        for v in [1i64, 2, 3, 4, 5, 6] {
            assert_eq!(
                route(&schema, "users", v),
                Shard::Direct(bigint(v) as usize % 2),
                "id={v}"
            );
        }
    }
}

mod mappings {
    use super::*;

    #[test]
    fn test_inline_list_mapping_table_specific() {
        let schema = make_sharding_schema(
            vec![ShardedTableConfig {
                database: "db".into(),
                name: Some("users".into()),
                column: "id".into(),
                data_type: DataType::Bigint,
                mapping: Some(vec![
                    ShardedMappingConfig::List(ShardedMappingList {
                        values: [1, 2, 3].into_iter().map(FlexibleType::Integer).collect(),
                        shard: 0,
                    }),
                    ShardedMappingConfig::List(ShardedMappingList {
                        values: [4, 5, 6].into_iter().map(FlexibleType::Integer).collect(),
                        shard: 1,
                    }),
                ]),
                ..Default::default()
            }],
            vec![],
        );

        for v in [1, 2, 3] {
            assert_eq!(
                route(&schema, "users", v),
                Shard::Direct(0),
                "id={v} → shard 0"
            );
        }
        for v in [4, 5, 6] {
            assert_eq!(
                route(&schema, "users", v),
                Shard::Direct(1),
                "id={v} → shard 1"
            );
        }
    }

    #[test]
    fn test_inline_list_mapping_column_wide() {
        // No table name: mapping applies to any table sharded on column "id".
        let schema = make_sharding_schema(
            vec![ShardedTableConfig {
                database: "db".into(),
                column: "id".into(),
                data_type: DataType::Bigint,
                mapping: Some(vec![
                    ShardedMappingConfig::List(ShardedMappingList {
                        values: [1, 2, 3].into_iter().map(FlexibleType::Integer).collect(),
                        shard: 0,
                    }),
                    ShardedMappingConfig::List(ShardedMappingList {
                        values: [4, 5, 6].into_iter().map(FlexibleType::Integer).collect(),
                        shard: 1,
                    }),
                ]),
                ..Default::default()
            }],
            vec![],
        );

        for v in [1, 2, 3] {
            assert_eq!(route(&schema, "users", v), Shard::Direct(0));
            assert_eq!(route(&schema, "orders", v), Shard::Direct(0));
        }
        for v in [4, 5, 6] {
            assert_eq!(route(&schema, "users", v), Shard::Direct(1));
            assert_eq!(route(&schema, "orders", v), Shard::Direct(1));
        }
    }

    #[test]
    fn test_inline_range_mapping_table_specific() {
        let schema = make_sharding_schema(
            vec![ShardedTableConfig {
                database: "db".into(),
                name: Some("orders".into()),
                column: "id".into(),
                data_type: DataType::Bigint,
                mapping: Some(
                    (0..2)
                        .map(|s| {
                            ShardedMappingConfig::Range(ShardedMappingRange {
                                start: Some(FlexibleType::Integer(s * 50)),
                                end: Some(FlexibleType::Integer((s + 1) * 50)),
                                shard: s as usize,
                            })
                        })
                        .collect(),
                ),
                ..Default::default()
            }],
            vec![],
        );

        // [0, 50) → shard 0; [50, 100) → shard 1
        for v in [0, 25, 49] {
            assert_eq!(
                route(&schema, "orders", v),
                Shard::Direct(0),
                "id={v} → shard 0"
            );
        }
        for v in [50, 75, 99] {
            assert_eq!(
                route(&schema, "orders", v),
                Shard::Direct(1),
                "id={v} → shard 1"
            );
        }
    }

    #[test]
    fn test_inline_mapping_unmatched_value_routes_to_all() {
        // Range mapping with no Default entry: a value outside every range has no
        // home shard, so it must broadcast to all shards (Shard::All).
        let schema = make_sharding_schema(
            vec![ShardedTableConfig {
                database: "db".into(),
                name: Some("orders".into()),
                column: "id".into(),
                data_type: DataType::Bigint,
                mapping: Some(vec![
                    ShardedMappingConfig::Range(ShardedMappingRange {
                        start: Some(FlexibleType::Integer(0)),
                        end: Some(FlexibleType::Integer(50)),
                        shard: 0,
                    }),
                    ShardedMappingConfig::Range(ShardedMappingRange {
                        start: Some(FlexibleType::Integer(50)),
                        end: Some(FlexibleType::Integer(100)),
                        shard: 1,
                    }),
                ]),
                ..Default::default()
            }],
            vec![],
        );

        // In-range values still route directly.
        assert_eq!(route(&schema, "orders", 25), Shard::Direct(0));
        // Above the top range and below the bottom range fall through to all shards.
        assert_eq!(
            route(&schema, "orders", 1000),
            Shard::All,
            "above max → all"
        );
        assert_eq!(route(&schema, "orders", -1), Shard::All, "below min → all");
    }

    #[test]
    fn test_inline_list_duplicate_value_last_wins() {
        // A value present in two list entries is not rejected (uniqueness is not
        // validated); Mapping::new folds entries into an IndexMap, so the LAST
        // insertion wins. This pins that documented behavior.
        let schema = make_sharding_schema(
            vec![ShardedTableConfig {
                database: "db".into(),
                name: Some("users".into()),
                column: "id".into(),
                data_type: DataType::Bigint,
                mapping: Some(vec![
                    ShardedMappingConfig::List(ShardedMappingList {
                        values: [1, 2].into_iter().map(FlexibleType::Integer).collect(),
                        shard: 0,
                    }),
                    ShardedMappingConfig::List(ShardedMappingList {
                        values: [2].into_iter().map(FlexibleType::Integer).collect(),
                        shard: 1,
                    }),
                ]),
                ..Default::default()
            }],
            vec![],
        );

        // 1 appears only in the first entry.
        assert_eq!(route(&schema, "users", 1), Shard::Direct(0));
        // 2 appears in both; the last entry (shard 1) wins.
        assert_eq!(
            route(&schema, "users", 2),
            Shard::Direct(1),
            "duplicate value → last entry wins"
        );
    }
}
