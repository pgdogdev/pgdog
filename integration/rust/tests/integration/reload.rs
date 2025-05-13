use std::time::Duration;

use rust::setup::{admin_tokio, backends, connection_failover};
use serial_test::serial;
use sqlx::Executor;
use tokio::time::sleep;

#[tokio::test]
#[serial]
async fn test_reload() {
    sleep(Duration::from_secs(1)).await;
    let admin = admin_tokio().await;
    let conn = connection_failover().await;

    conn.execute("SET application_name TO 'test_reload'")
        .await
        .unwrap();
    conn.execute("SELECT 1").await.unwrap();

    let backends_before = backends("test_reload", &conn).await;

    assert!(!backends_before.is_empty());

    for _ in 0..5 {
        conn.execute("SELECT 1").await.unwrap();
        admin.simple_query("RELOAD").await.unwrap();
        sleep(Duration::from_millis(50)).await;
    }

    let backends_after = backends("test_reload", &conn).await;

    let some_survived = backends_after.iter().any(|b| backends_before.contains(b));
    assert!(some_survived);
}

#[tokio::test]
#[serial]
async fn test_reconnect() {
    let admin = admin_tokio().await;
    let conn = connection_failover().await;

    conn.execute("SET application_name TO 'test_reconnect'")
        .await
        .unwrap();
    conn.execute("SELECT 1").await.unwrap(); // Trigger param update.

    let backends_before = backends("test_reconnect", &conn).await;

    assert!(!backends_before.is_empty());

    conn.execute("SELECT 1").await.unwrap();
    admin.simple_query("RECONNECT").await.unwrap();
    sleep(Duration::from_millis(50)).await;

    let backends_after = backends("test_reconnect", &conn).await;

    let none_survived = backends_after.iter().any(|b| backends_before.contains(b));
    assert!(!none_survived);
}
