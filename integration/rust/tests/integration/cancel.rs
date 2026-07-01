use std::time::Duration;

use crate::setup::{admin_tokio, connection_sqlx_direct};
use bytes::{BufMut, BytesMut};
use sqlx::PgPool;
use tokio::{io::AsyncWriteExt, net::TcpStream, task::JoinHandle, time::timeout};
use tokio_postgres::{CancelToken, Error as PgError, NoTls, SimpleQueryMessage};

/// Returns whether `pid` has an active `pg_sleep` query visible in `pg_stat_activity`.
/// Uses a direct PostgreSQL connection so the result bypasses pgdog completely.
async fn is_sleeping(direct: &PgPool, pid: i32) -> bool {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) \
         FROM pg_stat_activity \
         WHERE pid = $1 \
           AND state = 'active' \
           AND query LIKE '%pg_sleep%'",
    )
    .bind(pid)
    .fetch_one(direct)
    .await
    .unwrap();
    count == 1
}

/// Connect to pgdog, pin to a specific PG backend via BEGIN, capture the backend pid
/// via `pg_backend_pid()`, and launch `SELECT pg_sleep(60)` in a background task.
///
/// `application_name` is embedded in the connection string so the caller can identify
/// this connection in `SHOW CLIENTS` if needed.
///
/// Returns `(backend_pid, cancel_token, query_handle)`. The caller owns `cancel_token`
/// and `query_handle`; both must be driven to completion to keep the test clean.
async fn start_sleeping_connection(
    application_name: &str,
) -> (
    i32,
    CancelToken,
    JoinHandle<Result<Vec<SimpleQueryMessage>, PgError>>,
) {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 user=pgdog dbname=pgdog password=pgdog port=6432 application_name={application_name}"
        ),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("pgdog connection error: {}", e);
        }
    });

    let cancel_token = client.cancel_token();

    // BEGIN pins the client to one backend for the duration of the transaction.
    // Without this, transaction-mode pooling may assign a different backend to
    // pg_sleep than the one whose pid we captured.
    client.simple_query("BEGIN").await.unwrap();

    let row = client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap();
    let backend_pid: i32 = row.get(0);

    let handle = tokio::spawn(async move { client.simple_query("SELECT pg_sleep(60)").await });

    (backend_pid, cancel_token, handle)
}

/// Assert that a query handle returned by `start_sleeping_connection` was cancelled:
/// it must resolve to SQLSTATE 57014 (canceling statement due to user request).
async fn assert_cancelled(
    handle: JoinHandle<Result<Vec<SimpleQueryMessage>, PgError>>,
    label: &str,
) {
    let result = timeout(Duration::from_secs(5), handle)
        .await
        .unwrap_or_else(|_| panic!("{label}: cancelled query did not unblock within 5 seconds"))
        .unwrap_or_else(|_| panic!("{label}: task panicked"));

    let err = result.expect_err(&format!(
        "{label}: query should have been cancelled, but it succeeded"
    ));
    let db_err = err
        .as_db_error()
        .unwrap_or_else(|| panic!("{label}: expected a PostgreSQL error, not a network error"));

    assert_eq!(
        db_err.code().code(),
        "57014",
        "{label}: expected SQLSTATE 57014, got {}",
        db_err.code().code()
    );
}

/// Verify that cancellation is precise: two independent connections both run a long
/// query and each cancel request stops exactly one of them.
///
/// Steps:
/// 1. Two clients connect through pgdog; each starts `SELECT pg_sleep(60)`.
/// 2. Both queries are confirmed active on specific PG backends via `pg_stat_activity`.
/// 3. Cancel connection 1 → only backend 1 stops; backend 2 remains active.
/// 4. Cancel connection 2 → backend 2 stops.
#[tokio::test]
async fn test_cancel_query() {
    let direct = connection_sqlx_direct().await;

    let (pid1, token1, handle1) = start_sleeping_connection("cancel_test").await;
    let (pid2, token2, handle2) = start_sleeping_connection("cancel_test").await;

    // Give both queries time to reach their respective backends.
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(
        is_sleeping(&direct, pid1).await,
        "connection 1 (backend {pid1}) should be active before any cancel"
    );
    assert!(
        is_sleeping(&direct, pid2).await,
        "connection 2 (backend {pid2}) should be active before any cancel"
    );

    // ── Cancel connection 1 ────────────────────────────────────────────────
    token1.cancel_query(NoTls).await.unwrap();

    // Wait for the client to receive the cancellation error.
    // By the time the handle resolves, the backend has already stopped.
    assert_cancelled(handle1, "connection 1").await;

    // Connection 1's backend is gone; connection 2 must still be running.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !is_sleeping(&direct, pid1).await,
        "backend {pid1} should be idle after cancelling connection 1"
    );
    assert!(
        is_sleeping(&direct, pid2).await,
        "backend {pid2} should still be active after cancelling connection 1 only"
    );

    // ── Cancel connection 2 ────────────────────────────────────────────────
    token2.cancel_query(NoTls).await.unwrap();

    assert_cancelled(handle2, "connection 2").await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !is_sleeping(&direct, pid2).await,
        "backend {pid2} should be idle after cancelling connection 2"
    );
}

/// Verify that a cancel request carrying a wrong pid and secret is silently rejected:
/// the running query is unaffected and the client does not receive a cancellation error.
///
/// pgdog's `verify_cancel` gate must reject the request before it reaches the pool,
/// so the backend continues executing as if nothing happened.
#[tokio::test]
async fn test_cancel_query_wrong_secret() {
    let direct = connection_sqlx_direct().await;
    let app_name = "cancel_test_wrong_secret";
    let (backend_pid, real_cancel_token, query_handle) = start_sleeping_connection(app_name).await;

    // Give the query time to reach the backend.
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(
        is_sleeping(&direct, backend_pid).await,
        "query should be running before wrong-secret cancel"
    );

    // Look up the pgdog client pid from the admin interface.
    // SHOW CLIENTS exposes the pid (the 'id' column) that pgdog assigned during login —
    // the same value that was sent in the K message and that verify_cancel checks against.
    let admin = admin_tokio().await;
    let messages = admin.simple_query("SHOW CLIENTS").await.unwrap();
    let pgdog_pid: i32 = messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .find(|row| row.get("application_name") == Some(app_name))
        .expect("connection should appear in SHOW CLIENTS")
        .get("id")
        .expect("id column should be present")
        .parse()
        .expect("id should be a valid i32");

    // Send a CancelRequest with the real pgdog client pid but a wrong secret.
    // pgdog will find the client in comms by pid, then reject it because
    // the secret doesn't match — verify_cancel returns false.
    let mut raw = TcpStream::connect("127.0.0.1:6432").await.unwrap();
    let mut buf = BytesMut::new();
    buf.put_i32(16); // total message length (including the length field)
    buf.put_i32(80877102); // CancelRequest magic code
    buf.put_i32(pgdog_pid); // correct pid
    buf.put_i32(0); // wrong secret
    raw.write_all(&buf).await.unwrap();
    // pgdog closes the connection silently after processing; no response is sent.
    drop(raw);

    // Give pgdog enough time to receive and process the bogus cancel.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The query must still be running — the secret mismatch was caught by verify_cancel.
    assert!(
        is_sleeping(&direct, backend_pid).await,
        "query should still be running after wrong-secret cancel — verify_cancel must have rejected it"
    );

    // Clean up: cancel for real.
    real_cancel_token.cancel_query(NoTls).await.unwrap();
    assert_cancelled(query_handle, "wrong-secret test cleanup").await;
}
