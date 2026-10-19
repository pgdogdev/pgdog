use crate::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Executor, Row, postgres::PgPool};

#[tokio::test]
async fn having_filters_merged_groups() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();
    reset_table(&sharded).await?;

    seed(
        &sharded,
        0,
        "INSERT INTO having_merge_test VALUES (1, 10), (1, 11), (2, 12)",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO having_merge_test VALUES (1, 20), (2, 21)",
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

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns().len(), 2);
    assert_eq!(rows[0].get::<i64, _>("customer_id"), 1);
    assert_eq!(rows[0].get::<i64, _>("order_count"), 3);

    cleanup(&sharded).await;
    Ok(())
}

#[tokio::test]
async fn prepared_having_parameter_is_typed_by_postgres() -> Result<(), Box<dyn std::error::Error>>
{
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();
    reset_table(&sharded).await?;

    seed(
        &sharded,
        0,
        "INSERT INTO having_merge_test VALUES (1, 10), (1, 11)",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO having_merge_test VALUES (1, 20), (2, 21)",
    )
    .await?;

    let rows = sqlx::query(
        "SELECT customer_id, COUNT(*) AS order_count \
         FROM having_merge_test \
         GROUP BY customer_id \
         HAVING COUNT(*) > $1",
    )
    .bind(2_i64)
    .fetch_all(&sharded)
    .await?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("customer_id"), 1);
    assert_eq!(rows[0].get::<i64, _>("order_count"), 3);

    cleanup(&sharded).await;
    Ok(())
}

async fn reset_table(pool: &PgPool) -> Result<(), sqlx::Error> {
    cleanup(pool).await;
    for shard in [0, 1] {
        pool.execute(
            format!(
                "/* pgdog_shard: {shard} */ \
                 CREATE TABLE having_merge_test(customer_id BIGINT, order_id BIGINT)"
            )
            .as_str(),
        )
        .await?;
    }
    admin_sqlx().await.execute("RELOAD").await?;
    Ok(())
}

async fn seed(pool: &PgPool, shard: usize, statement: &str) -> Result<(), sqlx::Error> {
    pool.execute(format!("/* pgdog_shard: {shard} */ {statement}").as_str())
        .await?;
    Ok(())
}

async fn cleanup(pool: &PgPool) {
    for shard in [0, 1] {
        pool.execute(
            format!("/* pgdog_shard: {shard} */ DROP TABLE IF EXISTS having_merge_test").as_str(),
        )
        .await
        .ok();
    }
}
