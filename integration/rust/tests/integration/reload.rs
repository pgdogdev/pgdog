use std::time::Duration;

use rust::setup::{admin_tokio, connection_failover};
use sqlx::Executor;
use tokio::time::sleep;

#[tokio::test]
async fn test_reload() {
    let admin = admin_tokio().await;
    let conn = connection_failover().await;

    for _ in 0..5 {
        conn.execute("SELECT 1").await.unwrap();
        admin.simple_query("RELOAD").await.unwrap();
        sleep(Duration::from_millis(100)).await;
    }
}
