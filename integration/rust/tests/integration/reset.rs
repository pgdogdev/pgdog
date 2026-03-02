use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::Executor;

async fn run_reset_single_param() {
    let pools = connections_sqlx().await;
    let sharded = &pools[1];

    let mut conn = sharded.acquire().await.unwrap();

    // Set a parameter
    conn.execute("SET statement_timeout TO '5000'")
        .await
        .unwrap();

    // Verify it's set
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(timeout, "5s", "statement_timeout should be 5s after SET");

    // Reset the parameter
    conn.execute("RESET statement_timeout").await.unwrap();

    // Verify it's reset to default
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        timeout, "0",
        "statement_timeout should be 0 (default) after RESET"
    );
}

#[tokio::test]
async fn test_reset_single_param() {
    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn(run_reset_single_param()));
    }
    for handle in handles {
        handle.await.unwrap();
    }
}

async fn run_reset_all() {
    let pools = connections_sqlx().await;
    let sharded = &pools[1];

    let mut conn = sharded.acquire().await.unwrap();

    // Set multiple parameters
    conn.execute("SET statement_timeout TO '5000'")
        .await
        .unwrap();
    conn.execute("SET lock_timeout TO '3000'").await.unwrap();

    // Verify they're set
    let statement_timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(statement_timeout, "5s");

    let lock_timeout: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(lock_timeout, "3s");

    // Reset all parameters
    conn.execute("RESET ALL").await.unwrap();

    // Verify they're reset to defaults
    let statement_timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        statement_timeout, "0",
        "statement_timeout should be 0 after RESET ALL"
    );

    let lock_timeout: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        lock_timeout, "0",
        "lock_timeout should be 0 after RESET ALL"
    );
}

#[tokio::test]
async fn test_reset_all() {
    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn(run_reset_all()));
    }
    for handle in handles {
        handle.await.unwrap();
    }
}

async fn run_reset_in_transaction_commit() {
    let pools = connections_sqlx().await;
    let sharded = &pools[1];

    let mut conn = sharded.acquire().await.unwrap();

    // Set a parameter outside transaction
    conn.execute("SET statement_timeout TO '5000'")
        .await
        .unwrap();

    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(timeout, "5s");

    // Begin transaction and reset
    conn.execute("BEGIN").await.unwrap();
    conn.execute("RESET statement_timeout").await.unwrap();

    // Verify it's reset inside transaction
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(timeout, "0", "should be reset inside transaction");

    // Commit
    conn.execute("COMMIT").await.unwrap();

    // Verify it stays reset after commit
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(timeout, "0", "should stay reset after COMMIT");
}

#[tokio::test]
async fn test_reset_in_transaction_commit() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET cross_shard_disabled TO true")
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn(run_reset_in_transaction_commit()));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    admin
        .execute("SET cross_shard_disabled TO false")
        .await
        .unwrap();
}

async fn run_reset_in_transaction_rollback() {
    let pools = connections_sqlx().await;
    let sharded = &pools[1];

    let mut conn = sharded.acquire().await.unwrap();

    // Set a parameter outside transaction
    conn.execute("SET statement_timeout TO '5000'")
        .await
        .unwrap();

    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(timeout, "5s");

    // Begin transaction and reset
    conn.execute("BEGIN").await.unwrap();
    conn.execute("RESET statement_timeout").await.unwrap();

    // Verify it's reset inside transaction
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(timeout, "0", "should be reset inside transaction");

    // Rollback
    conn.execute("ROLLBACK").await.unwrap();

    // Verify it's restored after rollback
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(timeout, "5s", "should be restored after ROLLBACK");
}

#[tokio::test]
async fn test_reset_in_transaction_rollback() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET cross_shard_disabled TO true")
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn(run_reset_in_transaction_rollback()));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    admin
        .execute("SET cross_shard_disabled TO false")
        .await
        .unwrap();
}
