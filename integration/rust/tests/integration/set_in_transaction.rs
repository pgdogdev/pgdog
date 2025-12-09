use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::Executor;

async fn run_set_in_transaction_reset_after_commit() {
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

    // Verify lock_timeout is preserved after commit
    let timeout_after_commit: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_ne!(
        timeout_after_commit, original_timeout,
        "lock_timeout should be preserved after commit"
    );
    assert_eq!(
        timeout_after_commit, new_timeout,
        "lock_timeout should be preserved after commit"
    );
}

#[tokio::test]
async fn test_set_in_transaction_reset_after_commit() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET cross_shard_disabled TO true")
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn(run_set_in_transaction_reset_after_commit()));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    admin
        .execute("SET cross_shard_disabled TO false")
        .await
        .unwrap();
}

async fn run_set_in_transaction_reset_after_rollback() {
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
}

#[tokio::test]
async fn test_set_in_transaction_reset_after_rollback() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET cross_shard_disabled TO true")
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn(run_set_in_transaction_reset_after_rollback()));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    admin
        .execute("SET cross_shard_disabled TO false")
        .await
        .unwrap();
}

async fn run_set_local_in_transaction_reset_after_commit() {
    let pools = connections_sqlx().await;
    let sharded = &pools[1];

    let mut conn = sharded.acquire().await.unwrap();

    // Get the original work_mem before any transaction
    let original_work_mem: String = sqlx::query_scalar("SHOW work_mem")
        .fetch_one(&mut *conn)
        .await
        .unwrap();

    // Make sure we set it to something different
    let new_work_mem = if original_work_mem == "8MB" {
        "16MB"
    } else {
        "8MB"
    };

    // Start a transaction and change work_mem using SET LOCAL
    conn.execute("BEGIN").await.unwrap();
    conn.execute(format!("SET LOCAL work_mem TO '{}'", new_work_mem).as_str())
        .await
        .unwrap();

    // Verify work_mem is set inside transaction
    let work_mem_in_tx: String = sqlx::query_scalar("SHOW work_mem")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        work_mem_in_tx, new_work_mem,
        "work_mem should be {} inside transaction",
        new_work_mem
    );

    conn.execute("COMMIT").await.unwrap();

    // Verify work_mem is reset to original after commit (SET LOCAL is transaction-scoped)
    let work_mem_after_commit: String = sqlx::query_scalar("SHOW work_mem")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        work_mem_after_commit, original_work_mem,
        "work_mem should be reset to original after commit (SET LOCAL is transaction-scoped)"
    );
}

#[tokio::test]
async fn test_set_local_in_transaction_reset_after_commit() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET cross_shard_disabled TO true")
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn(
            run_set_local_in_transaction_reset_after_commit(),
        ));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    admin
        .execute("SET cross_shard_disabled TO false")
        .await
        .unwrap();
}
