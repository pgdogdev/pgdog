use crate::setup::admin_sqlx;
use crate::setup::connections_sqlx;
use sqlx::{Executor, Row, postgres::PgPool};

#[tokio::test]
async fn limit_without_order_by_across_shards() -> Result<(), Box<dyn std::error::Error>> {
    let pools = connections_sqlx().await;
    let mut transaction = pools[1].begin().await?;

    transaction.execute("TRUNCATE sharded").await?;
    // Guarantee five rows on each shard so a per-shard LIMIT exceeds the global limit.
    for shard in [0, 1] {
        transaction
            .execute(
                format!(
                    "/* pgdog_shard: {shard} */ INSERT INTO sharded (id) VALUES (1), (2), (3), (4), (5)"
                )
                .as_str(),
            )
            .await?;
    }

    for (limit, expected) in [(5_i64, 5), (0, 0), (100, 10)] {
        let rows = sqlx::raw_sql(&format!("SELECT id FROM sharded LIMIT {limit}"))
            .fetch_all(&mut *transaction)
            .await?;
        assert_eq!(rows.len(), expected, "simple protocol LIMIT {limit}");

        let rows = sqlx::query("SELECT id FROM sharded LIMIT $1")
            .bind(limit)
            .fetch_all(&mut *transaction)
            .await?;
        assert_eq!(rows.len(), expected, "extended protocol LIMIT {limit}");
    }

    transaction.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn limit_across_shards() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "limit_test",
        "(id SERIAL, value INTEGER, customer_id BIGINT)",
    )
    .await?;

    // Insert 5 rows per shard (10 total)
    seed(
        &sharded,
        0,
        "INSERT INTO limit_test(value) VALUES (1), (2), (3), (4), (5)",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO limit_test(value) VALUES (6), (7), (8), (9), (10)",
    )
    .await?;

    // LIMIT 5
    let rows = sharded
        .fetch_all("SELECT value FROM limit_test ORDER BY value LIMIT 5")
        .await?;
    assert_eq!(rows.len(), 5);
    for (i, row) in rows.iter().enumerate() {
        let value: i32 = row.get("value");
        assert_eq!(value, (i + 1) as i32);
    }

    // LIMIT 0
    let rows = sharded
        .fetch_all("SELECT value FROM limit_test ORDER BY value LIMIT 0")
        .await?;
    assert_eq!(rows.len(), 0);

    // LIMIT larger than result set
    let rows = sharded
        .fetch_all("SELECT value FROM limit_test ORDER BY value LIMIT 100")
        .await?;
    assert_eq!(rows.len(), 10);

    cleanup(&sharded, "limit_test").await;
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
