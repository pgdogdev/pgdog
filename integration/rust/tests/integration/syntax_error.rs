use rust::setup::connections_sqlx;
use sqlx::Executor;

/// Make sure we don't get disconnected on syntax error.
#[tokio::test]
async fn test_syntax_error() {
    let conns = connections_sqlx().await;

    for conn in conns {
        for _ in 0..25 {
            let res = conn.execute("SELECT FROM syntax_error WHERE").await;
            assert!(res.is_err());
            let res = conn.execute("SELECT 1").await;
            assert!(res.is_ok());
        }
    }
}
