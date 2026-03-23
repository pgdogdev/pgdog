use rust::setup::{admin_sqlx, admin_tokio};
use serial_test::serial;
use sqlx::{Executor, Row};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::NoTls;

async fn connect_tenant(db: &str) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 user=pgdog dbname={} password=pgdog port=6432",
            db
        ),
        NoTls,
    )
    .await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

async fn count_wildcard_pools(prefix: &str) -> usize {
    let admin = admin_sqlx().await;
    let pools = admin.fetch_all("SHOW POOLS").await.unwrap();
    pools
        .iter()
        .map(|p| p.get::<String, &str>("database"))
        .filter(|db| db.starts_with(prefix))
        .collect::<HashSet<_>>()
        .len()
}

#[tokio::test]
#[serial]
#[ignore = "requires wildcard config and tenant databases in Postgres"]
async fn pool_cap_basic() {
    let admin = admin_tokio().await;
    admin
        .simple_query("SET max_wildcard_pools TO 5")
        .await
        .unwrap();
    sleep(Duration::from_millis(200)).await;

    let mut clients = Vec::new();
    for i in 1..=5 {
        let db = format!("tenant_cap_{i}");
        let client = connect_tenant(&db)
            .await
            .unwrap_or_else(|e| panic!("tenant_cap_{i} should connect: {e}"));
        client.simple_query("SELECT 1").await.unwrap();
        clients.push(client);
    }

    // 6th connection should be rejected — pool cap reached.
    let result = connect_tenant("tenant_cap_6").await;
    match result {
        Ok(client) => {
            let q = client.simple_query("SELECT 1").await;
            assert!(q.is_err(), "query on 6th tenant should fail when cap is 5");
        }
        Err(_) => { /* connection-level rejection is also acceptable */ }
    }

    assert_eq!(count_wildcard_pools("tenant_cap_").await, 5);

    admin.simple_query("RELOAD").await.unwrap();
    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
#[serial]
#[ignore = "requires wildcard config and tenant databases in Postgres"]
async fn pool_cap_no_corruption() {
    let admin = admin_tokio().await;
    admin
        .simple_query("SET max_wildcard_pools TO 3")
        .await
        .unwrap();
    sleep(Duration::from_millis(200)).await;

    let mut clients = Vec::new();
    for i in 1..=3 {
        let db = format!("tenant_nocorr_{i}");
        let client = connect_tenant(&db).await.unwrap();
        client.simple_query("SELECT 1").await.unwrap();
        clients.push(client);
    }

    // 4th should be rejected.
    let overflow = connect_tenant("tenant_nocorr_4").await;
    match overflow {
        Ok(c) => assert!(c.simple_query("SELECT 1").await.is_err()),
        Err(_) => {}
    }

    // Original 3 pools must still function after the rejection.
    for client in &clients {
        client
            .simple_query("SELECT 1")
            .await
            .expect("existing wildcard pool should still work after cap rejection");
    }

    admin.simple_query("RELOAD").await.unwrap();
    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
#[serial]
#[ignore = "requires wildcard config and tenant databases in Postgres"]
async fn pool_cap_after_eviction() {
    let admin = admin_tokio().await;
    admin
        .simple_query("SET max_wildcard_pools TO 3")
        .await
        .unwrap();
    admin
        .simple_query("SET wildcard_pool_idle_timeout TO 2")
        .await
        .unwrap();
    sleep(Duration::from_millis(200)).await;

    // Connect and immediately drop so pools become idle.
    for i in 1..=3 {
        let db = format!("tenant_evict_{i}");
        let client = connect_tenant(&db).await.unwrap();
        client.simple_query("SELECT 1").await.unwrap();
    }

    // Wait for the eviction loop (timeout=2s + margin).
    sleep(Duration::from_secs(4)).await;

    // Old pools should be evicted; new ones should succeed.
    for i in 4..=6 {
        let db = format!("tenant_evict_{i}");
        let client = connect_tenant(&db)
            .await
            .unwrap_or_else(|e| panic!("tenant_evict_{i} should connect after eviction: {e}"));
        client.simple_query("SELECT 1").await.unwrap();
    }

    admin.simple_query("RELOAD").await.unwrap();
    sleep(Duration::from_millis(200)).await;
}
