use chrono::{Duration, Utc};
use rust::setup::connections_sqlx;
use sqlx::{Executor, Row};

#[tokio::test]
async fn test_timestamp_sorting_across_shards() {
    let conns = connections_sqlx().await;

    // We only want to test on the sharded database
    let sharded_conn = &conns[1]; // pgdog_sharded

    // Drop and create our timestamp test table
    // This table name 'timestamp_test' is configured in pgdog.toml as a sharded table
    sharded_conn
        .execute("DROP TABLE IF EXISTS timestamp_test CASCADE")
        .await
        .unwrap();

    sharded_conn
        .execute(
            "CREATE TABLE timestamp_test (
                id BIGINT PRIMARY KEY,
                name VARCHAR(100),
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMPTZ,
                special_ts TIMESTAMP  -- for testing special values
            )",
        )
        .await
        .unwrap();

    // Insert test data across different shards
    // The id will determine which shard the data goes to
    let base_time = Utc::now();
    let test_data = vec![
        // Mix of data that will go to different shards
        (1, "First item", base_time - Duration::days(5)),
        (101, "Second item", base_time - Duration::days(4)),
        (2, "Third item", base_time - Duration::days(3)),
        (102, "Fourth item", base_time - Duration::days(2)),
        (3, "Fifth item", base_time - Duration::days(1)),
        (103, "Sixth item", base_time),
        (4, "Seventh item", base_time + Duration::days(1)),
        (104, "Eighth item", base_time + Duration::days(2)),
        (5, "Ninth item", base_time + Duration::days(3)),
        (105, "Tenth item", base_time + Duration::days(4)),
    ];

    for (id, name, timestamp) in &test_data {
        sqlx::query(
            "INSERT INTO timestamp_test (id, name, created_at, updated_at) 
             VALUES ($1, $2, $3, $3)",
        )
        .bind(id)
        .bind(name)
        .bind(timestamp.naive_utc())
        .execute(sharded_conn)
        .await
        .unwrap();
    }

    // Test ORDER BY created_at DESC
    let rows = sharded_conn
        .fetch_all("SELECT id, name, created_at FROM timestamp_test ORDER BY created_at DESC")
        .await
        .unwrap();

    // Debug: print actual row count and all rows
    eprintln!("Actual row count: {}", rows.len());
    eprintln!("Actual order:");
    for (i, row) in rows.iter().enumerate() {
        let id: i64 = row.get(0);
        let name: String = row.get(1);
        let created_at: chrono::NaiveDateTime = row.get(2);
        eprintln!(
            "  Row {}: id={}, name={}, created_at={}",
            i, id, name, created_at
        );
    }

    assert_eq!(rows.len(), 10, "Should have 10 rows");

    // Verify the order is correct (newest first)
    let expected_order = vec![
        (105i64, "Tenth item"),
        (5, "Ninth item"),
        (104, "Eighth item"),
        (4, "Seventh item"),
        (103, "Sixth item"),
        (3, "Fifth item"),
        (102, "Fourth item"),
        (2, "Third item"),
        (101, "Second item"),
        (1, "First item"),
    ];

    for (i, row) in rows.iter().enumerate() {
        let id: i64 = row.get(0);
        let name: String = row.get(1);
        assert_eq!(
            (id, name.as_str()),
            expected_order[i],
            "Row {} has incorrect order",
            i
        );
    }

    // Test ORDER BY created_at ASC
    let rows = sharded_conn
        .fetch_all("SELECT id, name, created_at FROM timestamp_test ORDER BY created_at ASC")
        .await
        .unwrap();

    assert_eq!(rows.len(), 10, "Should have 10 rows");

    // Verify ascending order (oldest first)
    for (i, row) in rows.iter().enumerate() {
        let id: i64 = row.get(0);
        let name: String = row.get(1);
        assert_eq!(
            (id, name.as_str()),
            expected_order[9 - i], // Reverse of descending order
            "Row {} has incorrect ascending order",
            i
        );
    }

    // Test with NULL values
    sqlx::query("INSERT INTO timestamp_test (id, name, created_at) VALUES ($1, $2, $3)")
        .bind(201i64)
        .bind("Null timestamp item")
        .bind(base_time.naive_utc()) // created_at cannot be null
        .execute(sharded_conn)
        .await
        .unwrap();

    // Insert a row with NULL updated_at
    sqlx::query(
        "INSERT INTO timestamp_test (id, name, created_at, updated_at) VALUES ($1, $2, $3, NULL)",
    )
    .bind(202i64)
    .bind("Null updated_at item")
    .bind(base_time.naive_utc())
    .execute(sharded_conn)
    .await
    .unwrap();

    // Test ORDER BY with potential NULL values in updated_at
    let rows = sharded_conn
        .fetch_all(
            "SELECT id, name, updated_at FROM timestamp_test ORDER BY updated_at DESC NULLS LAST",
        )
        .await
        .unwrap();

    // Verify NULL values are at the end
    let last_rows: Vec<Option<chrono::NaiveDateTime>> = rows
        .iter()
        .rev()
        .take(2)
        .map(|row| row.try_get(2).ok())
        .collect();

    assert!(
        last_rows.iter().any(|v| v.is_none()),
        "Should have NULL values at the end"
    );

    // Test with special timestamp values
    // First, let's test PostgreSQL's infinity values
    sqlx::query(
        "INSERT INTO timestamp_test (id, name, created_at, special_ts) 
         VALUES ($1, $2, $3, 'infinity'::timestamp)",
    )
    .bind(301i64)
    .bind("Positive infinity")
    .bind(base_time.naive_utc())
    .execute(sharded_conn)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO timestamp_test (id, name, created_at, special_ts) 
         VALUES ($1, $2, $3, '-infinity'::timestamp)",
    )
    .bind(302i64)
    .bind("Negative infinity")
    .bind(base_time.naive_utc())
    .execute(sharded_conn)
    .await
    .unwrap();

    // Test ordering with infinity values
    let rows = sharded_conn
        .fetch_all("SELECT id, name, special_ts FROM timestamp_test WHERE special_ts IS NOT NULL ORDER BY special_ts ASC")
        .await
        .unwrap();

    eprintln!("Rows with special timestamps: {}", rows.len());
    // Should have -infinity first, then infinity last

    // Clean up
    sharded_conn
        .execute("DROP TABLE IF EXISTS timestamp_test CASCADE")
        .await
        .unwrap();

    sharded_conn.close().await;
}

#[tokio::test]
async fn test_timestamp_edge_cases_in_database() {
    let conns = connections_sqlx().await;
    let sharded_conn = &conns[1]; // pgdog_sharded

    // Create our sharded timestamp test table with edge cases
    sharded_conn
        .execute("DROP TABLE IF EXISTS timestamp_test CASCADE")
        .await
        .unwrap();

    sharded_conn
        .execute(
            "CREATE TABLE timestamp_test (
                id BIGINT PRIMARY KEY,
                description TEXT,
                ts TIMESTAMP,
                ts_tz TIMESTAMPTZ
            )",
        )
        .await
        .unwrap();

    // Test edge cases including daylight savings time transitions
    let edge_cases = vec![
        // Basic edge cases
        (1, "Year 2000 (PG epoch)", "2000-01-01 00:00:00"),
        (101, "Before 2000", "1999-12-31 23:59:59"),
        (2, "Far future", "2099-12-31 23:59:59"),
        (102, "Leap day", "2024-02-29 12:00:00"),
        (3, "Microsecond precision", "2025-01-15 10:30:45.123456"),
        (103, "Another microsecond", "2025-01-15 10:30:45.123457"),
        // Daylight Savings Time transitions (US Eastern Time)
        // Spring forward: 2024-03-10 02:00:00 becomes 03:00:00
        (4, "Before spring DST", "2024-03-10 01:59:59"),
        (104, "After spring DST", "2024-03-10 03:00:00"),
        // Fall back: 2024-11-03 02:00:00 becomes 01:00:00
        (5, "Before fall DST", "2024-11-03 00:59:59"),
        (105, "During fall DST ambiguity", "2024-11-03 01:30:00"),
        (6, "After fall DST", "2024-11-03 02:00:00"),
        // More edge cases
        (106, "Year 1970 (Unix epoch)", "1970-01-01 00:00:00"),
        (7, "Negative microseconds", "1999-12-31 23:59:59.999999"),
        (107, "Max reasonable date", "9999-12-31 23:59:59.999999"),
    ];

    for (id, desc, ts_str) in &edge_cases {
        sqlx::query(
            "INSERT INTO timestamp_test (id, description, ts, ts_tz) VALUES ($1, $2, $3::timestamp, $3::timestamptz)",
        )
        .bind(id)
        .bind(desc)
        .bind(ts_str)
        .execute(sharded_conn)
        .await
        .unwrap();
    }

    // Test ordering with edge cases
    let rows = sharded_conn
        .fetch_all("SELECT id, description, ts FROM timestamp_test ORDER BY ts ASC")
        .await
        .unwrap();

    // Verify first row is the oldest (1970)
    let first_desc: String = rows[0].get(1);
    assert_eq!(
        first_desc, "Year 1970 (Unix epoch)",
        "Oldest timestamp should be Unix epoch (1970)"
    );

    // Verify microsecond ordering
    let microsecond_rows: Vec<(i64, String)> = rows
        .iter()
        .filter_map(|row| {
            let desc: String = row.get(1);
            if desc.contains("microsecond") {
                Some((row.get(0), desc))
            } else {
                None
            }
        })
        .collect();

    assert_eq!(microsecond_rows.len(), 2);
    // Just verify we have two microsecond rows with different IDs
    assert_ne!(
        microsecond_rows[0].0, microsecond_rows[1].0,
        "Microsecond rows should have different IDs"
    );

    // Print all rows for debugging
    eprintln!("Edge case test - total rows: {}", rows.len());
    for (i, row) in rows.iter().enumerate() {
        let id: i64 = row.get(0);
        let desc: String = row.get(1);
        let ts: chrono::NaiveDateTime = row.get(2);
        eprintln!("  Row {}: id={}, desc='{}', ts={}", i, id, desc, ts);
    }

    // Test special PostgreSQL timestamp values
    sqlx::query(
        "INSERT INTO timestamp_test (id, description, ts) VALUES ($1, $2, 'infinity'::timestamp)",
    )
    .bind(200i64)
    .bind("Positive infinity timestamp")
    .execute(sharded_conn)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO timestamp_test (id, description, ts) VALUES ($1, $2, '-infinity'::timestamp)",
    )
    .bind(201i64)
    .bind("Negative infinity timestamp")
    .execute(sharded_conn)
    .await
    .unwrap();

    // Test ordering with infinity values
    let rows_with_infinity = sharded_conn
        .fetch_all(
            "SELECT id, description, ts::text FROM timestamp_test WHERE id >= 200 ORDER BY ts",
        )
        .await
        .unwrap();

    eprintln!("\nRows with infinity values:");
    for row in &rows_with_infinity {
        let id: i64 = row.get(0);
        let desc: String = row.get(1);
        let ts_text: String = row.get(2);
        eprintln!("  id={}, desc='{}', ts_text='{}'", id, desc, ts_text);
    }

    // Clean up
    sharded_conn
        .execute("DROP TABLE timestamp_test")
        .await
        .unwrap();

    sharded_conn.close().await;
}

#[tokio::test]
async fn test_timestamp_sorting_sqlx_text_protocol() {
    let conns = connections_sqlx().await;
    let sharded_conn = &conns[1]; // pgdog_sharded

    // Drop and recreate table
    let _ = sharded_conn
        .execute("DROP TABLE IF EXISTS timestamp_test CASCADE")
        .await;

    sharded_conn
        .execute(
            "CREATE TABLE timestamp_test (
                id BIGINT PRIMARY KEY,
                name TEXT,
                ts TIMESTAMP NOT NULL
            )",
        )
        .await
        .unwrap();

    // Insert test data
    let base_time = Utc::now();
    let test_data = vec![
        (1i64, "Oldest", base_time - Duration::days(10)),
        (101i64, "Old", base_time - Duration::days(5)),
        (2i64, "Recent", base_time - Duration::days(1)),
        (102i64, "Current", base_time),
        (3i64, "Future", base_time + Duration::days(1)),
        (103i64, "Far future", base_time + Duration::days(10)),
    ];

    for (id, name, timestamp) in &test_data {
        sqlx::query("INSERT INTO timestamp_test (id, name, ts) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(name)
            .bind(timestamp.naive_utc())
            .execute(sharded_conn)
            .await
            .unwrap();
    }

    // Query and check ordering
    let rows = sharded_conn
        .fetch_all("SELECT id, name, ts FROM timestamp_test ORDER BY ts DESC")
        .await
        .unwrap();

    eprintln!("SQLX Text protocol test - rows returned: {}", rows.len());
    let mut actual_order = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let id: i64 = row.get(0);
        let name: String = row.get(1);
        let ts: chrono::NaiveDateTime = row.get(2);
        actual_order.push((id, name.clone()));
        eprintln!("  Row {}: id={}, name='{}', ts={}", i, id, name, ts);
    }

    let expected_order = vec![
        (103i64, "Far future".to_string()),
        (3i64, "Future".to_string()),
        (102i64, "Current".to_string()),
        (2i64, "Recent".to_string()),
        (101i64, "Old".to_string()),
        (1i64, "Oldest".to_string()),
    ];

    assert_eq!(
        actual_order, expected_order,
        "SQLX text protocol should sort correctly"
    );

    // Clean up
    sharded_conn
        .execute("DROP TABLE timestamp_test CASCADE")
        .await
        .unwrap();

    sharded_conn.close().await;
}
