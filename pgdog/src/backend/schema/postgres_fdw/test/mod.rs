//! Integration tests for postgres_fdw statement generation.

mod helpers;

use helpers::{find_table_columns, FdwTestFixture, TestDataType, TestPartitionStrategy};

use super::ForeignTableSchema;
use crate::backend::server::test::test_server;

/// Test a single-tier sharding scenario.
async fn run_single_tier_test(data_type: TestDataType, strategy: TestPartitionStrategy) {
    let mut server = test_server().await;
    let fixture = FdwTestFixture::new(
        &format!(
            "test_{}_{}",
            strategy.as_str().to_lowercase(),
            data_type.name()
        ),
        "shard_key",
        data_type,
        strategy,
    );

    // Create source table
    fixture.create_table(&mut server).await.unwrap();

    // Load schema
    let schema = ForeignTableSchema::load(&mut server).await.unwrap();

    // Find table columns
    let columns = find_table_columns(&schema, &fixture.table_name).expect("Table should be loaded");

    // Get schema name from loaded columns
    let schema_name = &columns.first().unwrap().schema_name;

    // Generate statements
    let sharding_schema = fixture.sharding_schema(schema_name);
    let result = fixture.generate_statements(columns, &sharding_schema);

    // Verify statement structure
    fixture.verify_statements(&result.statements);

    // Execute parent statement to verify SQL validity
    fixture
        .execute_parent_statement(&mut server, &result.statements)
        .await
        .expect("Parent statement should execute successfully");

    // Cleanup
    fixture.cleanup(&mut server).await.unwrap();
}

// Hash partitioning tests
#[tokio::test]
async fn test_hash_bigint() {
    run_single_tier_test(TestDataType::Bigint, TestPartitionStrategy::Hash).await;
}

#[tokio::test]
async fn test_hash_varchar() {
    run_single_tier_test(TestDataType::Varchar, TestPartitionStrategy::Hash).await;
}

#[tokio::test]
async fn test_hash_uuid() {
    run_single_tier_test(TestDataType::Uuid, TestPartitionStrategy::Hash).await;
}

// List partitioning tests
#[tokio::test]
async fn test_list_bigint() {
    run_single_tier_test(TestDataType::Bigint, TestPartitionStrategy::List).await;
}

#[tokio::test]
async fn test_list_varchar() {
    run_single_tier_test(TestDataType::Varchar, TestPartitionStrategy::List).await;
}

#[tokio::test]
async fn test_list_uuid() {
    run_single_tier_test(TestDataType::Uuid, TestPartitionStrategy::List).await;
}

// Range partitioning tests
#[tokio::test]
async fn test_range_bigint() {
    run_single_tier_test(TestDataType::Bigint, TestPartitionStrategy::Range).await;
}

#[tokio::test]
async fn test_range_varchar() {
    run_single_tier_test(TestDataType::Varchar, TestPartitionStrategy::Range).await;
}

#[tokio::test]
async fn test_range_uuid() {
    run_single_tier_test(TestDataType::Uuid, TestPartitionStrategy::Range).await;
}

// Existing tests refactored to use helpers

#[tokio::test]
async fn test_load_partitioned_table_schema() {
    let mut server = test_server().await;

    server
        .execute("DROP TABLE IF EXISTS test_partitioned_parent CASCADE")
        .await
        .unwrap();

    server
        .execute(
            "CREATE TABLE test_partitioned_parent (
                id BIGINT NOT NULL,
                created_at DATE NOT NULL,
                data TEXT
            ) PARTITION BY RANGE (created_at)",
        )
        .await
        .unwrap();

    server
        .execute(
            "CREATE TABLE test_partitioned_parent_2024 PARTITION OF test_partitioned_parent
            FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
        )
        .await
        .unwrap();

    server
        .execute(
            "CREATE TABLE test_partitioned_parent_2025 PARTITION OF test_partitioned_parent
            FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
        )
        .await
        .unwrap();

    let schema = ForeignTableSchema::load(&mut server).await.unwrap();

    let parent_cols = find_table_columns(&schema, "test_partitioned_parent");
    assert!(parent_cols.is_some(), "Parent table should be loaded");
    let parent_cols = parent_cols.unwrap();

    let first_parent_col = parent_cols.first().unwrap();
    assert!(!first_parent_col.is_partition);
    assert!(
        first_parent_col.partition_key.contains("RANGE"),
        "Parent should have RANGE partition key, got: {}",
        first_parent_col.partition_key
    );

    let child_2024 = find_table_columns(&schema, "test_partitioned_parent_2024");
    assert!(child_2024.is_some(), "Child 2024 should be loaded");
    let first_child = child_2024.unwrap().first().unwrap();
    assert!(first_child.is_partition);
    assert_eq!(first_child.parent_table_name, "test_partitioned_parent");

    server
        .execute("DROP TABLE IF EXISTS test_partitioned_parent CASCADE")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_two_tier_partitioning() {
    use crate::backend::pool::ShardingSchema;
    use crate::backend::replication::ShardedTables;
    use crate::config::{DataType, ShardedTable};

    let mut server = test_server().await;

    server
        .execute("DROP TABLE IF EXISTS test_two_tier CASCADE")
        .await
        .unwrap();
    server
        .execute("DROP TABLE IF EXISTS test_two_tier_fdw CASCADE")
        .await
        .unwrap();

    server
        .execute(
            "CREATE TABLE test_two_tier (
                id BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                created_at DATE NOT NULL
            ) PARTITION BY RANGE (created_at)",
        )
        .await
        .unwrap();

    server
        .execute(
            "CREATE TABLE test_two_tier_2024 PARTITION OF test_two_tier
            FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
        )
        .await
        .unwrap();

    server
        .execute(
            "CREATE TABLE test_two_tier_2025 PARTITION OF test_two_tier
            FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
        )
        .await
        .unwrap();

    let schema = ForeignTableSchema::load(&mut server).await.unwrap();

    let parent_cols = find_table_columns(&schema, "test_two_tier").expect("Parent should exist");
    let child_2024 = find_table_columns(&schema, "test_two_tier_2024")
        .expect("Child 2024 should exist")
        .clone();
    let child_2025 = find_table_columns(&schema, "test_two_tier_2025")
        .expect("Child 2025 should exist")
        .clone();

    let schema_name = &parent_cols.first().unwrap().schema_name;

    let sharded_table = ShardedTable {
        database: "test".into(),
        name: Some("test_two_tier".into()),
        schema: Some(schema_name.clone()),
        column: "customer_id".into(),
        data_type: DataType::Bigint,
        ..Default::default()
    };

    let tables: ShardedTables = [sharded_table].as_slice().into();
    let sharding_schema = ShardingSchema {
        shards: 2,
        tables,
        ..Default::default()
    };

    let result = super::ForeignTableBuilder::new(parent_cols, &sharding_schema)
        .with_children(vec![child_2024, child_2025])
        .build()
        .unwrap();

    // 7 statements: parent + 2*(intermediate + 2 foreign)
    assert_eq!(result.statements.len(), 7);

    // Parent uses original RANGE partitioning
    assert!(result.statements[0].contains("PARTITION BY RANGE"));

    // Intermediate partitions use HASH for sharding
    assert!(result.statements[1].contains("PARTITION BY HASH"));

    // Execute parent and intermediate statements
    let parent_stmt = result.statements[0].replace("test_two_tier", "test_two_tier_fdw");
    server.execute(&parent_stmt).await.unwrap();

    let intermediate = result.statements[1]
        .replace("test_two_tier_2024", "test_two_tier_fdw_2024")
        .replace("test_two_tier", "test_two_tier_fdw");
    server.execute(&intermediate).await.unwrap();

    server
        .execute("DROP TABLE IF EXISTS test_two_tier_fdw CASCADE")
        .await
        .unwrap();
    server
        .execute("DROP TABLE IF EXISTS test_two_tier CASCADE")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_load_foreign_table_schema() {
    let mut server = test_server().await;

    server
        .execute("DROP TABLE IF EXISTS test_fdw_schema")
        .await
        .unwrap();

    server
        .execute(
            "CREATE TABLE test_fdw_schema (
                id BIGINT NOT NULL,
                name VARCHAR(100) DEFAULT 'unknown',
                score NUMERIC(10, 2),
                created_at TIMESTAMP NOT NULL DEFAULT now()
            )",
        )
        .await
        .unwrap();

    let schema = ForeignTableSchema::load(&mut server).await.unwrap();

    let test_rows: Vec<_> = schema
        .tables()
        .values()
        .flatten()
        .filter(|r| r.table_name == "test_fdw_schema")
        .collect();

    assert_eq!(test_rows.len(), 4);

    let id_col = test_rows.iter().find(|r| r.column_name == "id").unwrap();
    assert!(id_col.is_not_null);

    server
        .execute("DROP TABLE IF EXISTS test_fdw_schema")
        .await
        .unwrap();
}
