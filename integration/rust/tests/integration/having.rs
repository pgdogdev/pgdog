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

#[tokio::test]
async fn having_boolean_expression_filters_merged_groups() -> Result<(), Box<dyn std::error::Error>>
{
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();
    reset_table(&sharded).await?;

    seed(
        &sharded,
        0,
        "INSERT INTO having_merge_test VALUES (1, 10), (1, 11), (2, 12), (3, 5)",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO having_merge_test VALUES (1, 20), (2, 21), (2, 22), (3, 6)",
    )
    .await?;

    let rows = sharded
        .fetch_all(
            "SELECT customer_id, COUNT(*) AS order_count \
             FROM having_merge_test \
             GROUP BY customer_id \
             HAVING (COUNT(*) > 2 AND NOT (MAX(order_id) > 21)) OR MIN(order_id) < 6 \
             ORDER BY customer_id",
        )
        .await?;

    let groups: Vec<(i64, i64)> = rows
        .iter()
        .map(|row| (row.get("customer_id"), row.get("order_count")))
        .collect();
    assert_eq!(groups, vec![(1, 3), (3, 2)]);
    assert!(rows.iter().all(|row| row.columns().len() == 2));

    cleanup(&sharded).await;
    Ok(())
}

#[tokio::test]
async fn having_null_semantics_on_merged_groups() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();
    reset_table(&sharded).await?;

    seed(
        &sharded,
        0,
        "INSERT INTO having_merge_test VALUES (1, 10), (2, 5), (3, NULL)",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO having_merge_test VALUES (1, 20), (3, NULL)",
    )
    .await?;

    let null_groups = sharded
        .fetch_all(
            "SELECT customer_id FROM having_merge_test \
             GROUP BY customer_id \
             HAVING MAX(order_id) IS NULL",
        )
        .await?;
    assert_eq!(null_groups.len(), 1);
    assert_eq!(null_groups[0].get::<i64, _>("customer_id"), 3);

    // NOT of an unknown comparison is still unknown, so customer 3 must be filtered out.
    let negated = sharded
        .fetch_all(
            "SELECT customer_id FROM having_merge_test \
             GROUP BY customer_id \
             HAVING NOT (MAX(order_id) > 15) \
             ORDER BY customer_id",
        )
        .await?;
    let customers: Vec<i64> = negated.iter().map(|row| row.get("customer_id")).collect();
    assert_eq!(customers, vec![2]);

    cleanup(&sharded).await;
    Ok(())
}

#[tokio::test]
async fn having_numeric_float_and_cast_operands_use_merged_values()
-> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();
    reset_table(&sharded).await?;

    seed(
        &sharded,
        0,
        "INSERT INTO having_merge_test VALUES \
         (1, 1, 50.00, 1.5), (2, 2, 40.00, 0.5), (3, 3, 1.00, 4.0)",
    )
    .await?;
    seed(
        &sharded,
        1,
        "INSERT INTO having_merge_test VALUES (1, 4, 49.995, 1.5), (2, 5, 10.00, 0.5)",
    )
    .await?;

    assert_eq!(
        matching_customers(&sharded, "SUM(price) > 99.99").await?,
        vec![1]
    );
    assert_eq!(
        matching_customers(&sharded, "SUM(weight) >= 3.0").await?,
        vec![1, 3]
    );
    assert_eq!(
        matching_customers(&sharded, "COUNT(*)::numeric > 1.5").await?,
        vec![1, 2]
    );

    cleanup(&sharded).await;
    Ok(())
}

async fn matching_customers(pool: &PgPool, having: &str) -> Result<Vec<i64>, sqlx::Error> {
    let rows = pool
        .fetch_all(
            format!(
                "SELECT customer_id FROM having_merge_test \
                 GROUP BY customer_id HAVING {having} ORDER BY customer_id"
            )
            .as_str(),
        )
        .await?;
    Ok(rows.iter().map(|row| row.get("customer_id")).collect())
}

async fn reset_table(pool: &PgPool) -> Result<(), sqlx::Error> {
    cleanup(pool).await;
    for shard in [0, 1] {
        pool.execute(
            format!(
                "/* pgdog_shard: {shard} */ \
                 CREATE TABLE having_merge_test(\
                     customer_id BIGINT, order_id BIGINT, price NUMERIC, weight DOUBLE PRECISION)"
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
