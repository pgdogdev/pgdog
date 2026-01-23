use rust::setup::connection_sqlx_direct;
use sqlx::{Executor, Postgres, pool::Pool, postgres::PgPoolOptions};

async fn sharded_pool() -> Pool<Postgres> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded?application_name=sqlx")
        .await
        .unwrap()
}

#[tokio::test]
#[ignore]
async fn test_unique_id() {
    let conn = sharded_pool().await;

    let row: (i64,) = sqlx::query_as("SELECT pgdog.unique_id()")
        .fetch_one(&conn)
        .await
        .unwrap();

    assert!(row.0 > 0, "unique_id should be positive");

    conn.close().await;
}

#[tokio::test]
async fn test_unique_id_multiple() {
    let conn = sharded_pool().await;

    let row: (i64, i64) = sqlx::query_as("SELECT pgdog.unique_id(), pgdog.unique_id()")
        .fetch_one(&conn)
        .await
        .unwrap();

    assert!(row.0 > 0, "first unique_id should be positive");
    assert!(row.1 > 0, "second unique_id should be positive");
    assert_ne!(row.0, row.1, "unique_ids should be different");

    conn.close().await;
}

#[tokio::test]
async fn test_unique_id_uniqueness() {
    let conn = sharded_pool().await;

    let mut ids = Vec::new();

    for _ in 0..100 {
        let row: (i64,) = sqlx::query_as("SELECT pgdog.unique_id()")
            .fetch_one(&conn)
            .await
            .unwrap();
        ids.push(row.0);
    }

    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 100, "all unique_ids should be unique");

    conn.close().await;
}

/// Test that pgdog.unique_id() PL/pgSQL function produces IDs with the same
/// bit layout as Rust's unique_id.rs implementation.
#[tokio::test]
async fn test_unique_id_bit_layout_matches_rust() {
    // Constants from Rust unique_id.rs - these must match the SQL implementation
    const SEQUENCE_BITS: u64 = 12;
    const NODE_BITS: u64 = 10;
    const NODE_SHIFT: u64 = SEQUENCE_BITS; // 12
    const TIMESTAMP_SHIFT: u64 = SEQUENCE_BITS + NODE_BITS; // 22
    const MAX_NODE_ID: u64 = (1 << NODE_BITS) - 1; // 1023
    const MAX_SEQUENCE: u64 = (1 << SEQUENCE_BITS) - 1; // 4095
    const PGDOG_EPOCH: u64 = 1764184395000;

    let conn = connection_sqlx_direct().await;

    // Run schema setup to ensure pgdog schema exists
    let setup_sql = include_str!("../../../../pgdog/src/backend/schema/setup.sql");
    conn.execute(setup_sql).await.expect("schema setup failed");

    // Configure pgdog.config with a known shard value
    let test_shard: i64 = 42;
    conn.execute("DELETE FROM pgdog.config")
        .await
        .expect("clear config");
    conn.execute(
        sqlx::query("INSERT INTO pgdog.config (shard, shards) VALUES ($1, 100)").bind(test_shard),
    )
    .await
    .expect("insert config");

    // Generate an ID using the SQL function
    let row: (i64,) = sqlx::query_as("SELECT pgdog.unique_id()")
        .fetch_one(&conn)
        .await
        .expect("generate unique_id");
    let id = row.0 as u64;

    // Extract components using the same bit layout as Rust
    let extracted_sequence = id & MAX_SEQUENCE;
    let extracted_node = (id >> NODE_SHIFT) & MAX_NODE_ID;
    let extracted_timestamp = id >> TIMESTAMP_SHIFT;

    // Verify node_id matches the configured shard
    assert_eq!(
        extracted_node, test_shard as u64,
        "node_id in generated ID should match configured shard"
    );

    // Verify timestamp is reasonable (after epoch, within a day)
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let expected_elapsed = now_ms - PGDOG_EPOCH;

    assert!(
        extracted_timestamp > 0,
        "timestamp should be positive (after epoch)"
    );
    // Timestamp should be close to current time (within 5 seconds)
    let diff = if extracted_timestamp > expected_elapsed {
        extracted_timestamp - expected_elapsed
    } else {
        expected_elapsed - extracted_timestamp
    };
    assert!(
        diff < 5000,
        "timestamp {} should be close to expected {} (diff: {}ms)",
        extracted_timestamp,
        expected_elapsed,
        diff
    );

    // Verify sequence is within valid range
    assert!(
        extracted_sequence <= MAX_SEQUENCE,
        "sequence {} should not exceed max {}",
        extracted_sequence,
        MAX_SEQUENCE
    );

    // Generate multiple IDs and verify they're monotonically increasing
    let mut prev_id = id;
    for _ in 0..100 {
        let row: (i64,) = sqlx::query_as("SELECT pgdog.unique_id()")
            .fetch_one(&conn)
            .await
            .unwrap();
        let new_id = row.0 as u64;
        assert!(
            new_id > prev_id,
            "IDs should be monotonically increasing: {} > {}",
            new_id,
            prev_id
        );
        prev_id = new_id;
    }

    conn.close().await;

    // Also test through pgdog (sharded pool) and verify bit layout matches
    let sharded = sharded_pool().await;

    let row: (i64,) = sqlx::query_as("SELECT pgdog.unique_id()")
        .fetch_one(&sharded)
        .await
        .expect("generate unique_id through pgdog");
    let pgdog_id = row.0 as u64;

    // Extract components from pgdog-generated ID
    let pgdog_sequence = pgdog_id & MAX_SEQUENCE;
    let pgdog_node = (pgdog_id >> NODE_SHIFT) & MAX_NODE_ID;
    let pgdog_timestamp = pgdog_id >> TIMESTAMP_SHIFT;

    // Verify node_id is valid (0 or 1 for sharded setup)
    assert!(
        pgdog_node <= MAX_NODE_ID,
        "pgdog node_id {} should be valid",
        pgdog_node
    );

    // Verify timestamp is close to current time
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let expected_elapsed = now_ms - PGDOG_EPOCH;
    let pgdog_diff = if pgdog_timestamp > expected_elapsed {
        pgdog_timestamp - expected_elapsed
    } else {
        expected_elapsed - pgdog_timestamp
    };
    assert!(
        pgdog_diff < 5000,
        "pgdog timestamp {} should be close to expected {} (diff: {}ms)",
        pgdog_timestamp,
        expected_elapsed,
        pgdog_diff
    );

    // Verify sequence is valid
    assert!(
        pgdog_sequence <= MAX_SEQUENCE,
        "pgdog sequence {} should not exceed max {}",
        pgdog_sequence,
        MAX_SEQUENCE
    );

    sharded.close().await;
}
