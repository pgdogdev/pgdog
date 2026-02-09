use rust::setup::connections_tokio;

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
async fn test_multi_set_mixed_returns_error() {
    for conn in connections_tokio().await {
        let err = conn
            .batch_execute("SET statement_timeout TO '10s'; SELECT 1")
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("multi-statement queries cannot mix SET with other commands"),
            "unexpected error: {err}",
        );
    }
}
