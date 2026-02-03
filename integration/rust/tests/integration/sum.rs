use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Connection, Executor, PgConnection, Row, postgres::PgPool};

const SHARD_URLS: [&str; 2] = [
    "postgres://pgdog:pgdog@127.0.0.1:5432/shard_0",
    "postgres://pgdog:pgdog@127.0.0.1:5432/shard_1",
];

#[tokio::test]
async fn sum_merges_across_shards() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "sum_test",
        "(price DOUBLE PRECISION, qty INTEGER, customer_id BIGINT)",
    )
    .await?;
    seed(
        &sharded,
        0,
        "INSERT INTO sum_test(price, qty) VALUES (10.0, 2), (14.0, 3)",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO sum_test(price, qty) VALUES (18.0, 5), (22.0, 7)",
    )
    .await?;

    let row = sharded
        .fetch_one("SELECT SUM(price) AS total_price, SUM(qty) AS total_qty FROM sum_test")
        .await?;

    let total_price: f64 = row.get("total_price");
    let total_qty: i64 = row.get("total_qty");

    assert!(
        (total_price - 64.0).abs() < 1e-9,
        "price sum mismatch: {}",
        total_price
    );
    assert_eq!(total_qty, 17, "qty sum mismatch");

    cleanup(&sharded, "sum_test").await;
    Ok(())
}

#[tokio::test]
async fn sum_with_filter() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "sum_filter_test",
        "(price DOUBLE PRECISION, customer_id BIGINT)",
    )
    .await?;
    seed(
        &sharded,
        0,
        "INSERT INTO sum_filter_test(price) VALUES (10.0), (14.0)",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO sum_filter_test(price) VALUES (18.0), (22.0)",
    )
    .await?;

    let row = sqlx::query("SELECT SUM(price) AS total FROM sum_filter_test WHERE price > $1")
        .bind(12.0_f64)
        .fetch_one(&sharded)
        .await?;

    let total: f64 = row.get("total");
    assert!(
        (total - 54.0).abs() < 1e-9,
        "filtered sum mismatch: {}",
        total
    );

    cleanup(&sharded, "sum_filter_test").await;
    Ok(())
}

#[tokio::test]
async fn sum_matches_direct_shard_queries() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "sum_verify_test",
        "(value DOUBLE PRECISION, customer_id BIGINT)",
    )
    .await?;
    seed(
        &sharded,
        0,
        "INSERT INTO sum_verify_test(value) VALUES (1.5), (2.5), (3.0)",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO sum_verify_test(value) VALUES (4.0), (5.5)",
    )
    .await?;

    let pgdog_sum: f64 = sharded
        .fetch_one("SELECT SUM(value) AS total FROM sum_verify_test")
        .await?
        .get("total");

    let expected = combined_sum("sum_verify_test", "value").await?;
    assert!(
        (pgdog_sum - expected).abs() < 1e-9,
        "pgdog={}, expected={}",
        pgdog_sum,
        expected
    );

    cleanup(&sharded, "sum_verify_test").await;
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

async fn combined_sum(table: &str, column: &str) -> Result<f64, sqlx::Error> {
    let mut total = 0.0;
    for url in SHARD_URLS {
        let mut conn = PgConnection::connect(url).await?;
        let query = format!("SELECT COALESCE(SUM({}), 0)::float8 FROM {}", column, table);
        let sum: f64 = sqlx::query_scalar(&query).fetch_one(&mut conn).await?;
        total += sum;
    }
    Ok(total)
}
