use crate::setup::connections_tokio;

fn extract_simple_query_value(msgs: &[tokio_postgres::SimpleQueryMessage]) -> String {
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row.get(0).unwrap().to_string();
        }
    }
    panic!("no row in simple_query response");
}

#[tokio::test]
async fn test_multi_set_simple_protocol() {
    for conn in connections_tokio().await {
        conn.batch_execute("SET statement_timeout TO '30s'; SET lock_timeout TO '10s'")
            .await
            .unwrap();

        let rows = conn.simple_query("SHOW statement_timeout").await.unwrap();
        assert_eq!(extract_simple_query_value(&rows), "30s");

        let rows = conn.simple_query("SHOW lock_timeout").await.unwrap();
        assert_eq!(extract_simple_query_value(&rows), "10s");
    }
}

#[tokio::test]
async fn test_multi_set_with_timezone_interval() {
    for conn in connections_tokio().await {
        conn.batch_execute(
            "SET client_min_messages TO warning;SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE",
        )
        .await
        .unwrap();

        let rows = conn.simple_query("SHOW client_min_messages").await.unwrap();
        assert_eq!(extract_simple_query_value(&rows), "warning");

        let rows = conn.simple_query("SHOW timezone").await.unwrap();
        let tz = extract_simple_query_value(&rows);
        assert!(
            tz.contains("00:00") || tz == "UTC" || tz == "Etc/UTC",
            "expected UTC-equivalent timezone, got {tz}",
        );
    }
}

#[tokio::test]
async fn test_multi_set_mixed_succeeds() {
    for conn in connections_tokio().await {
        conn.batch_execute("SET statement_timeout TO '10s'; SELECT 1")
            .await
            .unwrap();

        let rows = conn.simple_query("SHOW statement_timeout").await.unwrap();
        assert_eq!(extract_simple_query_value(&rows), "10s");
    }
}

#[tokio::test]
async fn test_multi_statement_select() {
    for conn in connections_tokio().await {
        let msgs = conn.simple_query("SELECT 1; SELECT 2").await.unwrap();
        let mut values: Vec<String> = msgs
            .iter()
            .filter_map(|m| {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = m {
                    row.get(0).map(|s| s.to_string())
                } else {
                    None
                }
            })
            .collect();
        // On a sharded connection, each scalar SELECT fans out to all shards, so
        // "SELECT 1; SELECT 2" may yield ["1","1","2","2"]. Dedup consecutive
        // duplicates to assert ordering: all SELECT 1 results before SELECT 2.
        values.dedup();
        assert_eq!(values, vec!["1", "2"]);
    }
}

#[tokio::test]
async fn test_multi_statement_transaction_batch() {
    for conn in connections_tokio().await {
        // Use a real table (not TEMP) — transaction-mode pooling may route the setup
        // query to a different backend than the BEGIN/COMMIT batch, so TEMP tables
        // (which are session-scoped) would not be visible.
        conn.batch_execute(
            "CREATE TABLE IF NOT EXISTS pgdog_multi_stmt_locks (
                key  TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                ttl  TIMESTAMPTZ NOT NULL
            )",
        )
        .await
        .unwrap();

        conn.batch_execute("TRUNCATE pgdog_multi_stmt_locks")
            .await
            .unwrap();

        conn.batch_execute(
            "BEGIN;\
             DELETE FROM pgdog_multi_stmt_locks WHERE ttl < CURRENT_TIMESTAMP AT TIME ZONE 'UTC';\
             INSERT INTO pgdog_multi_stmt_locks (key, owner, ttl) \
               VALUES ('test-key', 'test-owner', NOW() + INTERVAL '1 hour') \
               ON CONFLICT DO NOTHING;\
             COMMIT;",
        )
        .await
        .unwrap();

        let rows = conn
            .simple_query("SELECT owner FROM pgdog_multi_stmt_locks WHERE key = 'test-key'")
            .await
            .unwrap();
        let owner = extract_simple_query_value(&rows);
        assert_eq!(owner, "test-owner");
    }
}

/// PostgreSQL normally wraps a multi-statement simple query in an implicit transaction,
/// so a failure in any statement rolls back all preceding ones. PgDog splits the batch
/// into individual requests, meaning each statement runs with its own auto-commit.
/// This test documents that behavior: the first INSERT succeeds even when the second fails.
#[tokio::test]
async fn test_multi_statement_implicit_transaction_behavior() {
    for conn in connections_tokio().await {
        // Use a real table (not TEMP) — transaction-mode pooling may route the setup
        // query to a different backend than the INSERT batch, so TEMP tables
        // (which are session-scoped) would not be visible.
        conn.batch_execute(
            "CREATE TABLE IF NOT EXISTS pgdog_multi_stmt_implicit (id INT PRIMARY KEY)",
        )
        .await
        .unwrap();

        conn.batch_execute("TRUNCATE pgdog_multi_stmt_implicit")
            .await
            .unwrap();

        // Second INSERT violates the primary key. In standard PostgreSQL simple query
        // protocol both would roll back; with per-statement routing the first commits.
        let _ = conn
            .batch_execute(
                "INSERT INTO pgdog_multi_stmt_implicit VALUES (1); INSERT INTO pgdog_multi_stmt_implicit VALUES (1)",
            )
            .await;

        let rows = conn
            .simple_query("SELECT count(*) FROM pgdog_multi_stmt_implicit WHERE id = 1")
            .await
            .unwrap();
        let count = extract_simple_query_value(&rows);
        // The first INSERT committed; the second failed independently.
        assert_eq!(count, "1");
    }
}
