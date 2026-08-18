use crate::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Executor, Row, postgres::PgPool};

#[tokio::test]
async fn having_count_merges_across_shards() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "having_merge_test",
        "(customer_id BIGINT, order_id BIGINT)",
    )
    .await?;

    seed(
        &sharded,
        0,
        "INSERT INTO having_merge_test(customer_id, order_id) VALUES (1, 10), (1, 11), (2, 12)",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO having_merge_test(customer_id, order_id) VALUES (1, 20), (2, 21), (2, 22)",
    )
    .await?;

    let rows = sharded
        .fetch_all(
            "SELECT customer_id, COUNT(*) AS order_count \
             FROM having_merge_test \
             GROUP BY customer_id \
             HAVING COUNT(*) > 2 \
             ORDER BY customer_id",
        )
        .await?;

    assert_eq!(
        rows.len(),
        2,
        "global HAVING should keep both merged groups"
    );
    assert_eq!(rows[0].get::<i64, _>("customer_id"), 1);
    assert_eq!(rows[0].get::<i64, _>("order_count"), 3);
    assert_eq!(rows[1].get::<i64, _>("customer_id"), 2);
    assert_eq!(rows[1].get::<i64, _>("order_count"), 3);

    cleanup(&sharded, "having_merge_test").await;
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
