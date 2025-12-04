use rust::setup::{admin_sqlx, connections_sqlx};
use serial_test::serial;
use sqlx::Executor;

#[tokio::test]
#[serial]
async fn test_set_in_transaction_preserved_after_commit() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET cross_shard_disabled TO true")
        .await
        .unwrap();

    let pools = connections_sqlx().await;
    let sharded = &pools[1];

    let mut conn = sharded.acquire().await.unwrap();

    // Start a transaction and change lock_timeout
    conn.execute("BEGIN").await.unwrap();
    conn.execute("SET lock_timeout TO '45s'").await.unwrap();

    // Verify lock_timeout is set inside transaction
    let timeout_in_tx: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        timeout_in_tx, "45s",
        "lock_timeout should be 45s inside transaction"
    );

    conn.execute("COMMIT").await.unwrap();

    // Verify lock_timeout is preserved after commit
    let timeout_after_commit: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        timeout_after_commit, "45s",
        "lock_timeout should be preserved after commit"
    );

    admin
        .execute("SET cross_shard_disabled TO false")
        .await
        .unwrap();
}

#[tokio::test]
#[serial]
async fn test_set_in_transaction_discarded_after_rollback() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET cross_shard_disabled TO true")
        .await
        .unwrap();

    let pools = connections_sqlx().await;
    let sharded = &pools[1];

    let mut conn = sharded.acquire().await.unwrap();

    // Get the default statement_timeout before any transaction
    let default_timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();

    // Start a transaction and change statement_timeout
    conn.execute("BEGIN").await.unwrap();
    conn.execute("SET statement_timeout TO '30s'")
        .await
        .unwrap();

    // Verify statement_timeout is set inside transaction
    let timeout_in_tx: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        timeout_in_tx, "30s",
        "statement_timeout should be 30s inside transaction"
    );

    conn.execute("ROLLBACK").await.unwrap();

    // Verify statement_timeout is back to default after rollback
    let timeout_after_rollback: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        timeout_after_rollback, default_timeout,
        "statement_timeout should be reset to default after rollback"
    );

    admin
        .execute("SET cross_shard_disabled TO false")
        .await
        .unwrap();
}
