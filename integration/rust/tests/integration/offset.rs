use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Executor, Row, postgres::PgPool};

const TABLE: &str = "offset_test";

async fn reset(pool: &PgPool) -> Result<(), sqlx::Error> {
    for shard in [0, 1] {
        pool.execute(format!("/* pgdog_shard: {shard} */ DROP TABLE IF EXISTS {TABLE}").as_str())
            .await
            .ok();
    }
    for shard in [0, 1] {
        pool.execute(
            format!(
                "/* pgdog_shard: {shard} */ CREATE TABLE {TABLE} (id SERIAL, value INTEGER, customer_id BIGINT)"
            )
            .as_str(),
        )
        .await?;
    }
    admin_sqlx().await.execute("RELOAD").await?;
    Ok(())
}

async fn seed(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Shard 0: values 1..=5
    pool.execute(
        format!("/* pgdog_shard: 0 */ INSERT INTO {TABLE}(value) VALUES (1), (2), (3), (4), (5)")
            .as_str(),
    )
    .await?;
    // Shard 1: values 6..=10
    pool.execute(
        format!("/* pgdog_shard: 1 */ INSERT INTO {TABLE}(value) VALUES (6), (7), (8), (9), (10)")
            .as_str(),
    )
    .await?;
    Ok(())
}

async fn cleanup(pool: &PgPool) {
    for shard in [0, 1] {
        pool.execute(format!("/* pgdog_shard: {shard} */ DROP TABLE {TABLE}").as_str())
            .await
            .ok();
    }
}

fn values(rows: &[sqlx::postgres::PgRow]) -> Vec<i32> {
    rows.iter().map(|r| r.get::<i32, _>("value")).collect()
}

#[tokio::test]
async fn offset_pagination_literals() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();
    reset(&sharded).await?;
    seed(&sharded).await?;

    // Page 1: OFFSET 0 LIMIT 3
    let rows = sharded
        .fetch_all(format!("SELECT value FROM {TABLE} ORDER BY value LIMIT 3 OFFSET 0").as_str())
        .await?;
    assert_eq!(values(&rows), vec![1, 2, 3]);

    // Page 2: OFFSET 3 LIMIT 3
    let rows = sharded
        .fetch_all(format!("SELECT value FROM {TABLE} ORDER BY value LIMIT 3 OFFSET 3").as_str())
        .await?;
    assert_eq!(values(&rows), vec![4, 5, 6]);

    // Page 3: OFFSET 6 LIMIT 3
    let rows = sharded
        .fetch_all(format!("SELECT value FROM {TABLE} ORDER BY value LIMIT 3 OFFSET 6").as_str())
        .await?;
    assert_eq!(values(&rows), vec![7, 8, 9]);

    // Page 4: OFFSET 9 LIMIT 3 (only 1 row left)
    let rows = sharded
        .fetch_all(format!("SELECT value FROM {TABLE} ORDER BY value LIMIT 3 OFFSET 9").as_str())
        .await?;
    assert_eq!(values(&rows), vec![10]);

    // Past the end
    let rows = sharded
        .fetch_all(format!("SELECT value FROM {TABLE} ORDER BY value LIMIT 3 OFFSET 10").as_str())
        .await?;
    assert!(rows.is_empty());

    cleanup(&sharded).await;
    Ok(())
}

#[tokio::test]
async fn offset_pagination_prepared() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();
    reset(&sharded).await?;
    seed(&sharded).await?;

    let sql = format!("SELECT value FROM {TABLE} ORDER BY value LIMIT $1 OFFSET $2");

    // Page 1
    let rows = sqlx::query(&sql)
        .bind(3i64)
        .bind(0i64)
        .fetch_all(&sharded)
        .await?;
    assert_eq!(values(&rows), vec![1, 2, 3]);

    // Page 2
    let rows = sqlx::query(&sql)
        .bind(3i64)
        .bind(3i64)
        .fetch_all(&sharded)
        .await?;
    assert_eq!(values(&rows), vec![4, 5, 6]);

    // Page 3
    let rows = sqlx::query(&sql)
        .bind(3i64)
        .bind(6i64)
        .fetch_all(&sharded)
        .await?;
    assert_eq!(values(&rows), vec![7, 8, 9]);

    // Partial last page
    let rows = sqlx::query(&sql)
        .bind(3i64)
        .bind(9i64)
        .fetch_all(&sharded)
        .await?;
    assert_eq!(values(&rows), vec![10]);

    // Past the end
    let rows = sqlx::query(&sql)
        .bind(3i64)
        .bind(10i64)
        .fetch_all(&sharded)
        .await?;
    assert!(rows.is_empty());

    cleanup(&sharded).await;
    Ok(())
}

#[tokio::test]
async fn offset_full_scan() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();
    reset(&sharded).await?;
    seed(&sharded).await?;

    // OFFSET 0 with LIMIT covering all rows returns everything in order.
    let rows = sharded
        .fetch_all(format!("SELECT value FROM {TABLE} ORDER BY value LIMIT 10 OFFSET 0").as_str())
        .await?;
    assert_eq!(values(&rows), vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    cleanup(&sharded).await;
    Ok(())
}

#[tokio::test]
async fn offset_large_offset() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();
    reset(&sharded).await?;
    seed(&sharded).await?;

    // OFFSET larger than total row count.
    let rows = sharded
        .fetch_all(format!("SELECT value FROM {TABLE} ORDER BY value LIMIT 5 OFFSET 100").as_str())
        .await?;
    assert!(rows.is_empty());

    cleanup(&sharded).await;
    Ok(())
}

#[tokio::test]
async fn offset_descending_order() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();
    reset(&sharded).await?;
    seed(&sharded).await?;

    let rows = sharded
        .fetch_all(
            format!("SELECT value FROM {TABLE} ORDER BY value DESC LIMIT 3 OFFSET 2").as_str(),
        )
        .await?;
    assert_eq!(values(&rows), vec![8, 7, 6]);

    let rows = sharded
        .fetch_all(
            format!("SELECT value FROM {TABLE} ORDER BY value DESC LIMIT 3 OFFSET 5").as_str(),
        )
        .await?;
    assert_eq!(values(&rows), vec![5, 4, 3]);

    cleanup(&sharded).await;
    Ok(())
}
