use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Acquire, Executor, Row};

#[tokio::test]
async fn per_stmt_routing() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    sharded
        .execute(
            "CREATE TABLE IF NOT EXISTS per_stmt_routing (customer_id BIGINT PRIMARY KEY, value VARCHAR)",
        )
        .await?;

    admin_sqlx().await.execute("RELOAD").await?;

    sharded.execute("TRUNCATE TABLE per_stmt_routing").await?;

    for i in 0..50 {
        sqlx::query("INSERT INTO per_stmt_routing (customer_id, value) VALUES ($1, $2)")
            .bind(i as i64)
            .bind(format!("test_{}", i))
            .execute(&sharded)
            .await?;
    }

    let mut conn = sharded.acquire().await?;
    let mut tx = conn.begin().await?;

    for i in 0..50 {
        // This will always return a row.
        sqlx::query("SELECT * FROM per_stmt_routing WHERE customer_id = $1")
            .bind(i as i64)
            .fetch_one(&mut *tx)
            .await?;
    }

    let rows = sqlx::query("SELECT * FROM per_stmt_routing")
        .fetch_all(&mut *tx)
        .await?;
    assert_eq!(rows.len(), 50);

    let count = sqlx::query("SELECT COUNT(*)::bigint FROM per_stmt_routing")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(count.get::<i64, _>(0), 50);

    for i in 50..100 {
        // No duplicate key violations.
        sqlx::query("INSERT INTO per_stmt_routing (customer_id, value) VALUES ($1, $2)")
            .bind(i as i64)
            .bind(format!("test_{}", i))
            .execute(&mut *tx)
            .await?;
    }

    tx.rollback().await?;

    Ok(())
}
