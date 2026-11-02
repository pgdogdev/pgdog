use crate::setup::{admin_sqlx, connections_sqlx, connections_tokio};
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

    // Merged groups: customer 1 has 3 orders (max 20, sum 31),
    // customer 2 has 2 orders (max 21, sum 33).
    for (having, expected) in [
        ("COUNT(*) > 1 AND MAX(order_id) > 20", vec![2]),
        ("COUNT(*) > 2 OR MAX(order_id) < 0", vec![1]),
        ("SUM(order_id) / COUNT(*) > 15", vec![2]),
        ("customer_id <> 1 OR MAX(order_id) IS NULL", vec![2]),
    ] {
        let rows = sharded
            .fetch_all(
                format!(
                    "SELECT customer_id FROM having_merge_test \
                     GROUP BY customer_id HAVING {having} ORDER BY customer_id"
                )
                .as_str(),
            )
            .await?;
        let customers = rows
            .iter()
            .map(|row| row.get::<i64, _>("customer_id"))
            .collect::<Vec<_>>();
        assert_eq!(customers, expected, "{having}");
    }

    // PostgreSQL evaluates the merged predicate and its errors reach the client.
    let error = sharded
        .fetch_all(
            "SELECT customer_id FROM having_merge_test \
             GROUP BY customer_id HAVING COUNT(*) / 0 > 1",
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("division by zero"), "{error}");
    sharded.execute("SELECT 1").await?;

    cleanup(&sharded).await;
    Ok(())
}

#[tokio::test]
async fn having_rejects_shard_local_steps() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();
    reset_table(&sharded).await?;
    let client = connections_tokio().await.remove(1);

    // Each of these would run on shards before PgDog merges the groups.
    for sql in [
        "SELECT * FROM having_merge_test GROUP BY customer_id, order_id HAVING COUNT(*) > 0",
        "SELECT customer_id FROM having_merge_test GROUP BY customer_id \
         HAVING COUNT(*) > 0 ORDER BY customer_id LIMIT 1",
        "SELECT DISTINCT customer_id FROM having_merge_test \
         GROUP BY customer_id HAVING COUNT(*) > 0",
        "EXECUTE having_prepared(0)",
    ] {
        if sql.starts_with("EXECUTE") {
            client
                .simple_query(
                    "PREPARE having_prepared AS SELECT customer_id FROM having_merge_test \
                     GROUP BY customer_id HAVING COUNT(*) > $1",
                )
                .await?;
        }
        let error = client.simple_query(sql).await.unwrap_err();
        let message = error.as_db_error().map(|error| error.message());
        assert!(
            message.is_some_and(|message| message.contains("cross-shard HAVING is unsupported")),
            "{sql}: {error}"
        );
    }
    client.simple_query("SELECT 1").await?;

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
