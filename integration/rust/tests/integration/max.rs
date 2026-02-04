use chrono::{DateTime, NaiveDateTime, Utc};
use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Connection, Executor, PgConnection, Row, postgres::PgPool};

const SHARD_URLS: [&str; 2] = [
    "postgres://pgdog:pgdog@127.0.0.1:5432/shard_0",
    "postgres://pgdog:pgdog@127.0.0.1:5432/shard_1",
];

#[tokio::test]
async fn max_merges_across_shards() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "max_test",
        "(price DOUBLE PRECISION, customer_id BIGINT)",
    )
    .await?;
    seed(
        &sharded,
        0,
        "INSERT INTO max_test(price) VALUES (10.0), (14.0)",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO max_test(price) VALUES (18.0), (22.0)",
    )
    .await?;

    let row = sharded
        .fetch_one("SELECT MAX(price) AS max_price FROM max_test")
        .await?;

    let max_price: f64 = row.get("max_price");
    assert!(
        (max_price - 22.0).abs() < 1e-9,
        "max mismatch: {}",
        max_price
    );

    cleanup(&sharded, "max_test").await;
    Ok(())
}

#[tokio::test]
async fn min_merges_across_shards() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "min_test",
        "(price DOUBLE PRECISION, customer_id BIGINT)",
    )
    .await?;
    seed(
        &sharded,
        0,
        "INSERT INTO min_test(price) VALUES (10.0), (14.0)",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO min_test(price) VALUES (18.0), (22.0)",
    )
    .await?;

    let row = sharded
        .fetch_one("SELECT MIN(price) AS min_price FROM min_test")
        .await?;

    let min_price: f64 = row.get("min_price");
    assert!(
        (min_price - 10.0).abs() < 1e-9,
        "min mismatch: {}",
        min_price
    );

    cleanup(&sharded, "min_test").await;
    Ok(())
}

#[tokio::test]
async fn max_min_combined() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "max_min_test",
        "(value INTEGER, customer_id BIGINT)",
    )
    .await?;
    seed(
        &sharded,
        0,
        "INSERT INTO max_min_test(value) VALUES (5), (15), (25)",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO max_min_test(value) VALUES (10), (20), (30)",
    )
    .await?;

    let row = sharded
        .fetch_one("SELECT MAX(value) AS mx, MIN(value) AS mn FROM max_min_test")
        .await?;

    let mx: i32 = row.get("mx");
    let mn: i32 = row.get("mn");
    assert_eq!(mx, 30);
    assert_eq!(mn, 5);

    cleanup(&sharded, "max_min_test").await;
    Ok(())
}

#[tokio::test]
async fn max_matches_direct_shard_queries() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "max_verify_test",
        "(value DOUBLE PRECISION, customer_id BIGINT)",
    )
    .await?;
    seed(
        &sharded,
        0,
        "INSERT INTO max_verify_test(value) VALUES (1.5), (99.9), (3.0)",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO max_verify_test(value) VALUES (4.0), (5.5)",
    )
    .await?;

    let pgdog_max: f64 = sharded
        .fetch_one("SELECT MAX(value) AS mx FROM max_verify_test")
        .await?
        .get("mx");

    let expected = combined_max("max_verify_test", "value").await?;
    assert!(
        (pgdog_max - expected).abs() < 1e-9,
        "pgdog={}, expected={}",
        pgdog_max,
        expected
    );

    cleanup(&sharded, "max_verify_test").await;
    Ok(())
}

#[tokio::test]
async fn max_timestamp() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "max_ts_test",
        "(created_at TIMESTAMP, customer_id BIGINT)",
    )
    .await?;
    seed(
        &sharded,
        0,
        "INSERT INTO max_ts_test(created_at) VALUES ('2024-01-01 10:00:00'), ('2024-01-15 12:00:00')",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO max_ts_test(created_at) VALUES ('2024-02-01 08:00:00'), ('2024-01-20 14:00:00')",
    )
    .await?;

    let row = sharded
        .fetch_one("SELECT MAX(created_at) AS max_ts, MIN(created_at) AS min_ts FROM max_ts_test")
        .await?;

    let max_ts: NaiveDateTime = row.get("max_ts");
    let min_ts: NaiveDateTime = row.get("min_ts");

    assert_eq!(
        max_ts,
        NaiveDateTime::parse_from_str("2024-02-01 08:00:00", "%Y-%m-%d %H:%M:%S")?
    );
    assert_eq!(
        min_ts,
        NaiveDateTime::parse_from_str("2024-01-01 10:00:00", "%Y-%m-%d %H:%M:%S")?
    );

    cleanup(&sharded, "max_ts_test").await;
    Ok(())
}

#[tokio::test]
async fn max_timestamptz() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "max_tstz_test",
        "(created_at TIMESTAMPTZ, customer_id BIGINT)",
    )
    .await?;
    seed(
        &sharded,
        0,
        "INSERT INTO max_tstz_test(created_at) VALUES ('2024-01-01 10:00:00+00'), ('2024-01-15 12:00:00+00')",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO max_tstz_test(created_at) VALUES ('2024-02-01 08:00:00+00'), ('2024-01-20 14:00:00+00')",
    )
    .await?;

    let row = sharded
        .fetch_one("SELECT MAX(created_at) AS max_ts, MIN(created_at) AS min_ts FROM max_tstz_test")
        .await?;

    let max_ts: DateTime<Utc> = row.get("max_ts");
    let min_ts: DateTime<Utc> = row.get("min_ts");

    assert_eq!(max_ts, "2024-02-01T08:00:00Z".parse::<DateTime<Utc>>()?);
    assert_eq!(min_ts, "2024-01-01T10:00:00Z".parse::<DateTime<Utc>>()?);

    cleanup(&sharded, "max_tstz_test").await;
    Ok(())
}

async fn reset_table(pool: &PgPool, table: &str, schema: &str) -> Result<(), sqlx::Error> {
    for shard in [0, 1] {
        pool.execute(
            format!(
                "/* pgdog_shard: {} */ DROP TABLE IF EXISTS {}",
                shard, table
            )
            .as_str(),
        )
        .await
        .ok();
    }
    for shard in [0, 1] {
        pool.execute(
            format!(
                "/* pgdog_shard: {} */ CREATE TABLE {} {}",
                shard, table, schema
            )
            .as_str(),
        )
        .await?;
    }
    admin_sqlx().await.execute("RELOAD").await?;
    Ok(())
}

async fn seed(pool: &PgPool, shard: usize, stmt: &str) -> Result<(), sqlx::Error> {
    pool.execute(format!("/* pgdog_shard: {} */ {}", shard, stmt).as_str())
        .await?;
    Ok(())
}

async fn cleanup(pool: &PgPool, table: &str) {
    for shard in [0, 1] {
        pool.execute(format!("/* pgdog_shard: {} */ DROP TABLE {}", shard, table).as_str())
            .await
            .ok();
    }
}

async fn combined_max(table: &str, column: &str) -> Result<f64, sqlx::Error> {
    let mut max_val: Option<f64> = None;
    for url in SHARD_URLS {
        let mut conn = PgConnection::connect(url).await?;
        let query = format!("SELECT MAX({})::float8 FROM {}", column, table);
        let val: Option<f64> = sqlx::query_scalar(&query).fetch_one(&mut conn).await?;
        if let Some(v) = val {
            max_val = Some(max_val.map_or(v, |m| m.max(v)));
        }
    }
    Ok(max_val.unwrap_or(0.0))
}
