use rust::setup::{admin_sqlx, admin_tokio};
use serial_test::serial;
use sqlx::{Executor, Row};
use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::NoTls;

async fn connection_tokio(db: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 user=pgdog dbname={} password=pgdog port=6432",
            db
        ),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
}

async fn get_pool_ids(database: &str) -> (i64, i64) {
    let admin = admin_sqlx().await;
    let pools = admin.fetch_all("SHOW POOLS").await.unwrap();

    let primary_id = pools
        .iter()
        .find(|p| {
            p.get::<String, &str>("database") == database
                && p.get::<String, &str>("user") == "pgdog"
                && p.get::<String, &str>("role") == "primary"
        })
        .map(|p| p.get::<i64, &str>("id"))
        .unwrap();

    let replica_id = pools
        .iter()
        .find(|p| {
            p.get::<String, &str>("database") == database
                && p.get::<String, &str>("user") == "pgdog"
                && p.get::<String, &str>("role") == "replica"
        })
        .map(|p| p.get::<i64, &str>("id"))
        .unwrap();

    (primary_id, replica_id)
}

async fn ban_pools(database: &str) {
    let admin = admin_sqlx().await;
    let (primary_id, replica_id) = get_pool_ids(database).await;

    admin
        .execute(format!("BAN {}", primary_id).as_str())
        .await
        .unwrap();
    admin
        .execute(format!("BAN {}", replica_id).as_str())
        .await
        .unwrap();
}

async fn unban_pools(database: &str) {
    let admin = admin_sqlx().await;
    let (primary_id, replica_id) = get_pool_ids(database).await;

    admin
        .execute(format!("UNBAN {}", primary_id).as_str())
        .await
        .unwrap();
    admin
        .execute(format!("UNBAN {}", replica_id).as_str())
        .await
        .unwrap();
}

#[tokio::test]
#[serial]
async fn test_client_connection_recovery_default() {
    let admin = admin_tokio().await;

    // Ensure recover mode (default)
    admin
        .simple_query("SET client_connection_recovery TO 'recover'")
        .await
        .unwrap();

    // Give pools time to reinitialize
    sleep(Duration::from_millis(200)).await;

    // Connection that we'll test recovery on
    let conn = connection_tokio("pgdog").await;

    // Ban both primary and replica pools to force Banned error
    ban_pools("pgdog").await;

    // Give ban time to take effect
    sleep(Duration::from_millis(50)).await;

    // This query should fail with Banned error because both pools are banned
    conn.simple_query("BEGIN").await.unwrap();
    let result = conn.simple_query("SELECT 1").await;
    assert!(result.is_err(), "Expected error when pools are banned");
    let err = result.unwrap_err();
    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("all replicas down"),
        "Expected 'all replicas down' error, got: {}",
        err
    );

    // Unban pools
    unban_pools("pgdog").await;

    // Give unban time to take effect
    sleep(Duration::from_millis(100)).await;

    // In recovery mode, the connection should still be usable
    let result = conn.simple_query("BEGIN").await;
    assert!(
        result.is_ok(),
        "Expected query to succeed after recovery, got: {:?}",
        result.err()
    );
    let result = conn.simple_query("SELECT 1").await;
    assert!(
        result.is_ok(),
        "Expected SELECT to succeed after recovery, got: {:?}",
        result.err()
    );
    conn.simple_query("COMMIT").await.unwrap();

    // Reset settings
    admin.simple_query("RELOAD").await.unwrap();
}

#[tokio::test]
#[serial]
async fn test_client_connection_recovery_drop() {
    let admin = admin_tokio().await;

    // Set drop mode - client should be disconnected on recoverable errors
    admin
        .simple_query("SET client_connection_recovery TO 'drop'")
        .await
        .unwrap();

    // Give pools time to reinitialize
    sleep(Duration::from_millis(200)).await;

    // Connection that we'll test drop behavior on
    let conn = connection_tokio("pgdog").await;

    // Ban both pools to force Banned error
    ban_pools("pgdog").await;

    // Give ban time to take effect
    sleep(Duration::from_millis(50)).await;

    // This query should fail because pools are banned
    conn.simple_query("BEGIN").await.unwrap();
    let result = conn.simple_query("SELECT 1").await;
    assert!(result.is_err(), "Expected error when pools are banned");

    // Unban pools
    unban_pools("pgdog").await;

    // Give unban time to take effect
    sleep(Duration::from_millis(100)).await;

    // In drop mode, the connection should be disconnected
    // Subsequent queries should fail because the connection is closed
    let result = conn.simple_query("BEGIN").await;
    assert!(
        result.is_err(),
        "Expected query to fail because connection was dropped"
    );

    // Verify that the error indicates connection was closed
    let err = result.unwrap_err();
    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("connection closed"),
        "Expected 'connection closed' error, got: {}",
        err
    );

    // Reset settings
    admin.simple_query("RELOAD").await.unwrap();
}
