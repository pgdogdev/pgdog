use rust::setup::{admin_sqlx, connections_sqlx};
use serial_test::serial;
use sqlx::Executor;

#[tokio::test]
#[serial]
async fn test_set_in_transaction_reset_after_commit() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET cross_shard_disabled TO true")
        .await
        .unwrap();

    let pools = connections_sqlx().await;
    let sharded = &pools[1];

    let mut conn = sharded.acquire().await.unwrap();

    // Get the original lock_timeout before any transaction
    let original_timeout: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();

    // Make sure we set it to something different
    let new_timeout = if original_timeout == "45s" {
        "30s"
    } else {
        "45s"
    };

    // Start a transaction and change lock_timeout
    conn.execute("BEGIN").await.unwrap();
    conn.execute(format!("SET lock_timeout TO '{}'", new_timeout).as_str())
        .await
        .unwrap();

    // Verify lock_timeout is set inside transaction
    let timeout_in_tx: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        timeout_in_tx, new_timeout,
        "lock_timeout should be {} inside transaction",
        new_timeout
    );

    conn.execute("COMMIT").await.unwrap();

    // Verify lock_timeout is reset to original after commit
    let timeout_after_commit: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        timeout_after_commit, original_timeout,
        "lock_timeout should be reset to original after commit"
    );

    admin
        .execute("SET cross_shard_disabled TO false")
        .await
        .unwrap();
}

#[tokio::test]
#[serial]
async fn test_set_in_transaction_reset_after_rollback() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET cross_shard_disabled TO true")
        .await
        .unwrap();

    let pools = connections_sqlx().await;
    let sharded = &pools[1];

    let mut conn = sharded.acquire().await.unwrap();

    // Get the original statement_timeout before any transaction
    let original_timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();

    // Make sure we set it to something different
    let new_timeout = if original_timeout == "30s" {
        "45s"
    } else {
        "30s"
    };

    // Start a transaction and change statement_timeout
    conn.execute("BEGIN").await.unwrap();
    conn.execute(format!("SET statement_timeout TO '{}'", new_timeout).as_str())
        .await
        .unwrap();

    // Verify statement_timeout is set inside transaction
    let timeout_in_tx: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        timeout_in_tx, new_timeout,
        "statement_timeout should be {} inside transaction",
        new_timeout
    );

    conn.execute("ROLLBACK").await.unwrap();

    // Verify statement_timeout is back to original after rollback
    let timeout_after_rollback: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        timeout_after_rollback, original_timeout,
        "statement_timeout should be reset to original after rollback"
    );

    admin
        .execute("SET cross_shard_disabled TO false")
        .await
        .unwrap();
}
