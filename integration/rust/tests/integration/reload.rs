use std::time::Duration;

use rust::setup::{admin_tokio, backends, connection_failover, connection_sqlx_direct};
use serial_test::serial;
use sqlx::{Executor, Row, postgres::PgPoolOptions};
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
    conn.close().await;
}

#[tokio::test]
#[serial]
async fn test_reload_connection_count_stable() {
    sleep(Duration::from_secs(1)).await;
    let admin = admin_tokio().await;
    let direct = connection_sqlx_direct().await;

    // Count PgDog connections on Postgres before reload.
    let before: i64 = direct
        .fetch_one("SELECT COUNT(*)::BIGINT FROM pg_stat_activity WHERE application_name = 'PgDog'")
        .await
        .unwrap()
        .get(0);

    assert!(before > 0, "expected pgdog connections before reload");
    eprintln!("connections before RELOAD: {before}");

    admin.simple_query("RELOAD").await.unwrap();
    sleep(Duration::from_millis(500)).await;

    let after: i64 = direct
        .fetch_one("SELECT COUNT(*)::BIGINT FROM pg_stat_activity WHERE application_name = 'PgDog'")
        .await
        .unwrap()
        .get(0);

    eprintln!("connections after RELOAD: {after}");

    assert_eq!(
        before, after,
        "connection count changed after RELOAD: before={before}, after={after}"
    );

    direct.close().await;
}

#[tokio::test]
#[serial]
async fn test_reload_pool_size_not_exceeded() {
    let pool_size: i64 = 50; // default_pool_size
    let num_clients = 20;
    let app_name = "test_reload_pool_limit";

    sleep(Duration::from_secs(1)).await;
    let admin = admin_tokio().await;
    let direct = connection_sqlx_direct().await;

    // Spin up clients that continuously run queries.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = vec![];

    for _ in 0..num_clients {
        let stop = stop.clone();
        handles.push(tokio::spawn(async move {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&format!(
                    "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?application_name={app_name}"
                ))
                .await
                .unwrap();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = pool.execute("SELECT 1").await;
                sleep(Duration::from_millis(10)).await;
            }
            pool.close().await;
        }));
    }

    // Let clients warm up.
    sleep(Duration::from_millis(500)).await;

    // Issue multiple reloads while clients are active and check pool size each time.
    for i in 0..10 {
        admin.simple_query("RELOAD").await.unwrap();
        sleep(Duration::from_millis(100)).await;

        let query = format!(
            "SELECT COUNT(*)::BIGINT FROM pg_stat_activity WHERE application_name = '{app_name}'"
        );
        let count: i64 = direct.fetch_one(query.as_str()).await.unwrap().get(0);

        eprintln!("reload {i}: {count} connections (pool_size={pool_size})");
        assert!(
            count <= pool_size,
            "pool size exceeded after reload {i}: {count} > {pool_size}"
        );
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles {
        h.await.unwrap();
    }

    direct.close().await;
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

    conn.close().await;
}
