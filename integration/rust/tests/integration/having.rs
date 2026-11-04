use crate::setup::{admin_sqlx, connections_sqlx, connections_tokio};
use sqlx::{Executor, Row, postgres::PgPool};

#[tokio::test]
async fn having_errors_reach_the_client() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = connections_sqlx().await.get(1).cloned().unwrap();
    reset_table(&sharded).await?;
    seed(
        &sharded,
        0,
        r#"INSERT INTO having_merge_test VALUES (1, 10, '{"a": 1}')"#,
    )
    .await?;
    seed(
        &sharded,
        1,
        r#"INSERT INTO having_merge_test VALUES (1, 20, '{"a": 1}')"#,
    )
    .await?;
    let client = connections_tokio().await.remove(1);

    for (sql, expected) in [
        // PostgreSQL evaluates the merged predicate and returns its error.
        (
            "SELECT customer_id FROM having_merge_test \
             GROUP BY customer_id HAVING COUNT(*) / 0 > 1",
            "division by zero",
        ),
        // jsonb operands can't be sent to the filter query as arrays.
        (
            "SELECT tag, COUNT(*) FROM having_merge_test GROUP BY tag HAVING tag IS NOT NULL",
            "cross-shard HAVING is unsupported: operands must have common built-in types",
        ),
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        assert_eq!(
            error.as_db_error().map(|error| error.message()),
            Some(expected),
            "{sql}"
        );
        client.simple_query("SELECT 1").await?;

        // Inside a transaction, the error aborts it like in PostgreSQL.
        client.simple_query("BEGIN").await?;
        client.simple_query(sql).await.unwrap_err();
        let error = client.simple_query("SELECT 1").await.unwrap_err();
        assert_eq!(
            error.code(),
            Some(&tokio_postgres::error::SqlState::IN_FAILED_SQL_TRANSACTION),
            "{sql}: {error}"
        );
        client.simple_query("ROLLBACK").await?;
        client.simple_query("SELECT 1").await?;
    }

    cleanup(&sharded).await;
    Ok(())
}

#[tokio::test]
async fn prepared_having_parameter_typed_and_untyped() -> Result<(), Box<dyn std::error::Error>> {
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
    assert_eq!(rows[0].columns().len(), 2);
    assert_eq!(rows[0].get::<i64, _>("customer_id"), 1);
    assert_eq!(rows[0].get::<i64, _>("order_count"), 3);

    // Parameters are renumbered in the merged filter query.
    let rows = sqlx::query(
        "SELECT customer_id FROM having_merge_test \
         GROUP BY customer_id HAVING MAX(order_id) > $2 AND COUNT(*) > $1",
    )
    .bind(0_i64)
    .bind(20_i64)
    .fetch_all(&sharded)
    .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("customer_id"), 2);

    // Parse without parameter types, so PostgreSQL has to infer $1.
    let client = connections_tokio().await.remove(1);
    let statement = client
        .prepare(
            "SELECT customer_id FROM having_merge_test \
             GROUP BY customer_id HAVING COUNT(*) > $1",
        )
        .await?;
    let rows = client.query(&statement, &[&2_i64]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 1);

    // LIMIT and OFFSET apply to the groups that pass, with PostgreSQL's semantics.
    let statement = client
        .prepare(
            "SELECT customer_id FROM having_merge_test \
             GROUP BY customer_id HAVING COUNT(*) >= $1 \
             ORDER BY customer_id DESC LIMIT $2 OFFSET $3",
        )
        .await?;
    for (limit, offset, expected) in [(Some(1_i64), 1_i64, vec![1_i64]), (None, 0, vec![2, 1])] {
        let rows = client.query(&statement, &[&1_i64, &limit, &offset]).await?;
        let customers = rows.iter().map(|row| row.get(0)).collect::<Vec<i64>>();
        assert_eq!(customers, expected, "LIMIT {limit:?} OFFSET {offset}");
    }

    cleanup(&sharded).await;
    Ok(())
}

async fn reset_table(pool: &PgPool) -> Result<(), sqlx::Error> {
    cleanup(pool).await;
    for shard in [0, 1] {
        pool.execute(
            format!(
                "/* pgdog_shard: {shard} */ \
                 CREATE TABLE having_merge_test(customer_id BIGINT, order_id BIGINT, tag JSONB)"
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
