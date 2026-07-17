use crate::setup::connections_tokio;

#[tokio::test]
async fn test_transaction_control() {
    let conns = connections_tokio().await;
    for conn in conns {
        let begin = conn.prepare("BEGIN").await.unwrap();
        assert!(begin.params().is_empty());
        assert!(begin.columns().is_empty());

        for _ in 0..25 {
            conn.execute("BEGIN", &[]).await.unwrap();
            conn.execute("COMMIT", &[]).await.unwrap();

            conn.execute("BEGIN", &[]).await.unwrap();
            conn.execute("ROLLBACK", &[]).await.unwrap();
        }
    }
}
