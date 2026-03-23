use rust::setup::{admin_sqlx, admin_tokio};
use serial_test::serial;
use sqlx::{Executor, Row};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::NoTls;

async fn try_connect(db: &str) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
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

async fn pool_databases() -> HashSet<String> {
    let admin = admin_sqlx().await;
    let pools = admin.fetch_all("SHOW POOLS").await.unwrap();
    pools
        .iter()
        .map(|p| p.get::<String, &str>("database"))
        .collect()
}

#[tokio::test]
#[serial]
#[ignore = "requires wildcard config and tenant_concurrent_1 database in Postgres"]
async fn concurrent_same_db() {
    let admin = admin_tokio().await;
    admin
        .simple_query("SET max_wildcard_pools TO 0")
        .await
        .unwrap();
    sleep(Duration::from_millis(200)).await;

    let db = "tenant_concurrent_1";
    let handles: Vec<_> = (0..20)
        .map(|_| {
            let db = db.to_string();
            tokio::spawn(async move {
                let client = try_connect(&db).await?;
                client.simple_query("SELECT 1").await?;
                Ok::<_, tokio_postgres::Error>(())
            })
        })
        .collect();

    let results = futures_util::future::join_all(handles).await;
    let successes = results.iter().filter(|r| matches!(r, Ok(Ok(())))).count();
    assert!(
        successes >= 18,
        "at least 18/20 concurrent connections to same db should succeed, got {successes}"
    );

    let pools = pool_databases().await;
    assert!(pools.contains(db), "SHOW POOLS should list a pool for {db}");

    admin.simple_query("RELOAD").await.unwrap();
    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
#[serial]
#[ignore = "requires wildcard config and 20 tenant databases in Postgres"]
async fn concurrent_different_dbs() {
    let admin = admin_tokio().await;
    admin
        .simple_query("SET max_wildcard_pools TO 0")
        .await
        .unwrap();
    sleep(Duration::from_millis(200)).await;

    let handles: Vec<_> = (0..20)
        .map(|i| {
            tokio::spawn(async move {
                let db = format!("tenant_diff_{i}");
                let client = try_connect(&db).await?;
                client.simple_query("SELECT 1").await?;
                Ok::<_, tokio_postgres::Error>(())
            })
        })
        .collect();

    let results = futures_util::future::join_all(handles).await;
    let successes = results.iter().filter(|r| matches!(r, Ok(Ok(())))).count();
    assert_eq!(
        successes, 20,
        "all 20 different-db connections should succeed"
    );

    let pools = pool_databases().await;
    for i in 0..20 {
        let db = format!("tenant_diff_{i}");
        assert!(pools.contains(&db), "pool for {db} should exist");
    }

    admin.simple_query("RELOAD").await.unwrap();
    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
#[serial]
#[ignore = "requires wildcard config and 5 tenant databases in Postgres"]
async fn concurrent_mixed() {
    let admin = admin_tokio().await;
    admin
        .simple_query("SET max_wildcard_pools TO 0")
        .await
        .unwrap();
    sleep(Duration::from_millis(200)).await;

    // 5 different databases, 10 concurrent connections each.
    let handles: Vec<_> = (0..50)
        .map(|i| {
            let db_idx = i % 5;
            tokio::spawn(async move {
                let db = format!("tenant_mixed_{db_idx}");
                let client = try_connect(&db).await?;
                client.simple_query("SELECT 1").await?;
                Ok::<_, tokio_postgres::Error>(())
            })
        })
        .collect();

    let results = futures_util::future::join_all(handles).await;
    let successes = results.iter().filter(|r| matches!(r, Ok(Ok(())))).count();
    assert_eq!(successes, 50, "all 50 mixed connections should succeed");

    let pools = pool_databases().await;
    let mixed_pools: HashSet<_> = pools
        .iter()
        .filter(|db| db.starts_with("tenant_mixed_"))
        .collect();
    assert_eq!(mixed_pools.len(), 5, "should have exactly 5 distinct pools");

    admin.simple_query("RELOAD").await.unwrap();
    sleep(Duration::from_millis(200)).await;
}
