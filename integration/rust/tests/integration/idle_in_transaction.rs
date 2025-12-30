use std::time::Duration;

use rust::setup::{admin_sqlx, connection_sqlx_direct, connections_sqlx};
use sqlx::{Executor, Row};
use tokio::time::sleep;

#[tokio::test]
async fn test_idle_in_transaction_timeout() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET client_idle_in_transaction_timeout TO 500")
        .await
        .unwrap();

    let conn_direct = connection_sqlx_direct().await;

    for conn in connections_sqlx().await {
        let mut conn = conn.acquire().await.unwrap();

        conn.execute("BEGIN").await.unwrap();
        let pid_before = conn
            .fetch_one("SELECT pg_backend_pid()")
            .await
            .unwrap()
            .get::<i32, _>(0);
        sleep(Duration::from_millis(750)).await;
        let err = conn.execute("SELECT 1").await.unwrap_err();
        assert!(err.to_string().contains("idle in transaction"));

        sleep(Duration::from_millis(500)).await;

        let (pid_after, query): (i32, String) =
            sqlx::query_as("SELECT pid, query FROM pg_stat_activity WHERE pid = $1")
                .bind(pid_before)
                .fetch_one(&conn_direct)
                .await
                .unwrap();

        assert_eq!(
            pid_before, pid_after,
            "expexted pooler not to cycle connection"
        );
        assert_eq!(query, "ROLLBACK", "expected a rollback on the connection",);
    }

    // Reset settings.
    admin.execute("RELOAD").await.unwrap();
}
