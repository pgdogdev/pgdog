use super::numeric_infinity::text_rows;
use crate::setup::{admin_tokio, connections_sqlx, connections_tokio};
use chrono::{Duration, Utc};
use sqlx::{Executor, Row};

#[tokio::test]
async fn test_timestamp_sorting_across_shards() {
    let conns = connections_sqlx().await;
    let sharded_conn = &conns[1];

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
                special_ts TIMESTAMP
            )",
        )
        .await
        .unwrap();

    let base_time = Utc::now();
    let test_data = vec![
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

    let rows = sharded_conn
        .fetch_all("SELECT id, name, created_at FROM timestamp_test ORDER BY created_at DESC")
        .await
        .unwrap();

    assert_eq!(rows.len(), 10, "Should have 10 rows");

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

    let rows = sharded_conn
        .fetch_all("SELECT id, name, created_at FROM timestamp_test ORDER BY created_at ASC")
        .await
        .unwrap();

    assert_eq!(rows.len(), 10, "Should have 10 rows");

    for (i, row) in rows.iter().enumerate() {
        let id: i64 = row.get(0);
        let name: String = row.get(1);
        assert_eq!(
            (id, name.as_str()),
            expected_order[9 - i],
            "Row {} has incorrect ascending order",
            i
        );
    }

    sqlx::query("INSERT INTO timestamp_test (id, name, created_at) VALUES ($1, $2, $3)")
        .bind(201i64)
        .bind("Null timestamp item")
        .bind(base_time.naive_utc())
        .execute(sharded_conn)
        .await
        .unwrap();

    sqlx::query(
        "INSERT INTO timestamp_test (id, name, created_at, updated_at) VALUES ($1, $2, $3, NULL)",
    )
    .bind(202i64)
    .bind("Null updated_at item")
    .bind(base_time.naive_utc())
    .execute(sharded_conn)
    .await
    .unwrap();

    let rows = sharded_conn
        .fetch_all("SELECT id, name, updated_at FROM timestamp_test ORDER BY updated_at ASC")
        .await
        .unwrap();

    let last_rows: Vec<Option<chrono::DateTime<chrono::Utc>>> = rows
        .iter()
        .rev()
        .take(2)
        .map(|row| row.try_get(2))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(
        last_rows.iter().any(|v| v.is_none()),
        "Should have NULL values at the end"
    );

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

    let _rows = sharded_conn
        .fetch_all("SELECT id, name, special_ts FROM timestamp_test WHERE special_ts IS NOT NULL ORDER BY special_ts ASC")
        .await
        .unwrap();

    sharded_conn
        .execute("DROP TABLE IF EXISTS timestamp_test CASCADE")
        .await
        .unwrap();

    sharded_conn.close().await;
}

#[tokio::test]
async fn test_timestamp_edge_cases_in_database() {
    let conns = connections_sqlx().await;
    let sharded_conn = &conns[1];

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

    let edge_cases = vec![
        (1, "Year 2000 (PG epoch)", "2000-01-01 00:00:00"),
        (101, "Before 2000", "1999-12-31 23:59:59"),
        (2, "Far future", "2099-12-31 23:59:59"),
        (102, "Leap day", "2024-02-29 12:00:00"),
        (3, "Microsecond precision", "2025-01-15 10:30:45.123456"),
        (103, "Another microsecond", "2025-01-15 10:30:45.123457"),
        (4, "Before spring DST", "2024-03-10 01:59:59"),
        (104, "After spring DST", "2024-03-10 03:00:00"),
        (5, "Before fall DST", "2024-11-03 00:59:59"),
        (105, "During fall DST ambiguity", "2024-11-03 01:30:00"),
        (6, "After fall DST", "2024-11-03 02:00:00"),
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

    let rows = sharded_conn
        .fetch_all("SELECT id, description, ts FROM timestamp_test ORDER BY ts ASC")
        .await
        .unwrap();

    let first_desc: String = rows[0].get(1);
    assert_eq!(
        first_desc, "Year 1970 (Unix epoch)",
        "Oldest timestamp should be Unix epoch (1970)"
    );

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
    assert_ne!(
        microsecond_rows[0].0, microsecond_rows[1].0,
        "Microsecond rows should have different IDs"
    );

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

    let _rows_with_infinity = sharded_conn
        .fetch_all(
            "SELECT id, description, ts::text FROM timestamp_test WHERE id >= 200 ORDER BY ts",
        )
        .await
        .unwrap();

    sharded_conn
        .execute("DROP TABLE timestamp_test")
        .await
        .unwrap();

    sharded_conn.close().await;
}

#[tokio::test]
async fn test_timestamp_sorting_sqlx_text_protocol() {
    let conns = connections_sqlx().await;
    let sharded_conn = &conns[1];

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

    let rows = sharded_conn
        .fetch_all("SELECT id, name, ts FROM timestamp_test ORDER BY ts DESC")
        .await
        .unwrap();

    let actual_order: Vec<(i64, String)> =
        rows.iter().map(|row| (row.get(0), row.get(1))).collect();

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

    sharded_conn
        .execute("DROP TABLE timestamp_test CASCADE")
        .await
        .unwrap();

    sharded_conn.close().await;
}

/// Timestamps merged across shards are re-encoded by PgDog; their text
/// must match Postgres, which omits a zero fraction and trims trailing zeros.
#[tokio::test]
async fn timestamp_text_matches_postgres_across_shards() -> Result<(), Box<dyn std::error::Error>> {
    let connections = connections_tokio().await;
    let sharded = &connections[1];
    sharded.batch_execute("SET TimeZone TO 'UTC'").await?;
    sharded
        .batch_execute("DROP TABLE IF EXISTS timestamp_text_test")
        .await?;
    sharded
        .batch_execute(
            "CREATE TABLE timestamp_text_test (customer_id BIGINT, t TIMESTAMP, tz TIMESTAMPTZ)",
        )
        .await?;
    admin_tokio().await.batch_execute("RELOAD").await?;

    sharded
        .batch_execute(
            "/* pgdog_shard: 0 */ INSERT INTO timestamp_text_test VALUES
             (1, '2013-07-01 10:00:00', '2013-07-01 10:00:00+00'),
             (2, '2013-07-01 10:00:00.5', '2013-07-01 10:00:00.5+00')",
        )
        .await?;
    sharded
        .batch_execute(
            "/* pgdog_shard: 1 */ INSERT INTO timestamp_text_test VALUES
             (101, '2013-07-01 10:00:00', '2013-07-01 10:00:00+00'),
             (102, '2013-07-01 10:00:00.000001', '2013-07-01 10:00:00.000001+00'),
             (103, '2013-07-02 23:59:59.12', '2013-07-02 23:59:59.12+00')",
        )
        .await?;

    assert_eq!(
        text_rows(
            sharded,
            "SELECT MIN(t), MAX(t), MIN(tz), MAX(tz) FROM timestamp_text_test",
        )
        .await?,
        [[
            "2013-07-01 10:00:00",
            "2013-07-02 23:59:59.12",
            "2013-07-01 10:00:00+00",
            "2013-07-02 23:59:59.12+00",
        ]]
    );
    assert_eq!(
        text_rows(
            sharded,
            "SELECT t, tz, COUNT(*) FROM timestamp_text_test GROUP BY t, tz ORDER BY t",
        )
        .await?,
        [
            ["2013-07-01 10:00:00", "2013-07-01 10:00:00+00", "2"],
            [
                "2013-07-01 10:00:00.000001",
                "2013-07-01 10:00:00.000001+00",
                "1"
            ],
            ["2013-07-01 10:00:00.5", "2013-07-01 10:00:00.5+00", "1"],
            ["2013-07-02 23:59:59.12", "2013-07-02 23:59:59.12+00", "1"],
        ]
    );

    sharded
        .batch_execute("DROP TABLE timestamp_text_test")
        .await?;
    Ok(())
}
