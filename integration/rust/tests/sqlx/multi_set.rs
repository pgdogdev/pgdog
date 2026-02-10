use rust::setup::connections_sqlx;
use sqlx::Executor;

#[tokio::test]
async fn test_multi_set() {
    for pool in connections_sqlx().await {
        let mut conn = pool.acquire().await.unwrap();

        conn.execute("SET statement_timeout TO '25s'; SET lock_timeout TO '15s'")
            .await
            .unwrap();

        let statement_timeout: String = sqlx::query_scalar("SHOW statement_timeout")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(statement_timeout, "25s");

        let lock_timeout: String = sqlx::query_scalar("SHOW lock_timeout")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(lock_timeout, "15s");
    }
}

#[tokio::test]
async fn test_multi_set_in_transaction() {
    for pool in connections_sqlx().await {
        let mut conn = pool.acquire().await.unwrap();

        let original_st: String = sqlx::query_scalar("SHOW statement_timeout")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        let original_lt: String = sqlx::query_scalar("SHOW lock_timeout")
            .fetch_one(&mut *conn)
            .await
            .unwrap();

        conn.execute("BEGIN").await.unwrap();
        conn.execute("SET statement_timeout TO '33s'; SET lock_timeout TO '22s'")
            .await
            .unwrap();

        let st: String = sqlx::query_scalar("SHOW statement_timeout")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(st, "33s");

        let lt: String = sqlx::query_scalar("SHOW lock_timeout")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(lt, "22s");

        conn.execute("ROLLBACK").await.unwrap();

        let st_after: String = sqlx::query_scalar("SHOW statement_timeout")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(st_after, original_st);

        let lt_after: String = sqlx::query_scalar("SHOW lock_timeout")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(lt_after, original_lt);
    }
}

#[tokio::test]
async fn test_multi_set_mixed_returns_error() {
    for pool in connections_sqlx().await {
        let mut conn = pool.acquire().await.unwrap();

        let err = conn
            .execute("SET statement_timeout TO '10s'; SELECT 1")
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("multi-statement queries cannot mix SET with other commands"),
            "unexpected error: {err}",
        );

        // Connection should still be usable after the error.
        let val: String = sqlx::query_scalar("SHOW server_version")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert!(!val.is_empty());
    }
}
