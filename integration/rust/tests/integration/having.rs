use crate::setup::{admin_sqlx, connections_sqlx, connections_tokio};
use sqlx::{Executor, postgres::PgPool};

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
