use std::time::Duration;

use rust::setup::{admin_sqlx, admin_tokio, connection_failover, connections_sqlx};
use serial_test::serial;
use sqlx::Executor;
use tokio::time::sleep;

#[tokio::test]
#[serial]
async fn test_maintenance_mode_client_queries() {
    let admin = admin_tokio().await;
    let conn = connection_failover().await;

    // Ensure maintenance mode is off initially
    admin.simple_query("MAINTENANCE OFF").await.unwrap();

    // Client should be able to execute queries normally
    conn.execute("SELECT 1").await.unwrap();

    // Turn on maintenance mode
    admin.simple_query("MAINTENANCE ON").await.unwrap();

    // Test that maintenance mode blocks new queries by timing how long they take
    let start = std::time::Instant::now();

    // Start a client query in background
    let conn_clone = conn.clone();
    let client_task = tokio::spawn(async move {
        conn_clone.execute("SELECT 2").await.unwrap();
    });

    // Wait a minimal amount then turn off maintenance mode
    sleep(Duration::from_millis(50)).await;
    admin.simple_query("MAINTENANCE OFF").await.unwrap();

    // Wait for the client task to complete
    tokio::time::timeout(Duration::from_secs(2), client_task)
        .await
        .unwrap()
        .unwrap();

    let elapsed = start.elapsed();

    // The query should have taken at least 30ms due to maintenance mode blocking
    assert!(elapsed >= Duration::from_millis(30));

    // Client should work normally after maintenance mode is turned off
    conn.execute("SELECT 3").await.unwrap();

    conn.close().await;
}

#[tokio::test]
#[serial]
async fn test_maintenance_mode_admin_connections() {
    let admin = admin_tokio().await;
    let admin_sqlx = admin_sqlx().await;

    // Turn on maintenance mode
    admin.simple_query("MAINTENANCE ON").await.unwrap();

    // Admin connections should work during maintenance mode
    admin.simple_query("SHOW POOLS").await.unwrap();
    admin_sqlx.execute("SHOW STATS").await.unwrap();

    // Admin should be able to turn maintenance mode off
    admin.simple_query("MAINTENANCE OFF").await.unwrap();

    // Verify it's off by turning it on and off again
    admin.simple_query("MAINTENANCE ON").await.unwrap();
    admin.simple_query("MAINTENANCE OFF").await.unwrap();

    admin_sqlx.close().await;
}

#[tokio::test]
#[serial]
async fn test_maintenance_mode_multiple_cycles() {
    let admin = admin_tokio().await;
    let conn = connection_failover().await;

    // Test multiple on/off cycles
    for i in 0..3 {
        // Ensure maintenance mode is off
        admin.simple_query("MAINTENANCE OFF").await.unwrap();

        // Connection should work when maintenance is off
        conn.execute(format!("SELECT {}", i * 2 + 1).as_str())
            .await
            .unwrap();

        // Turn on maintenance mode then quickly off
        admin.simple_query("MAINTENANCE ON").await.unwrap();
        admin.simple_query("MAINTENANCE OFF").await.unwrap();

        // Connection should work after maintenance is turned off
        conn.execute(format!("SELECT {}", i * 2 + 2).as_str())
            .await
            .unwrap();
    }

    conn.close().await;
}

#[tokio::test]
#[serial]
async fn test_maintenance_mode_parsing() {
    let admin = admin_tokio().await;

    // Test valid commands
    admin.simple_query("MAINTENANCE ON").await.unwrap();
    admin.simple_query("MAINTENANCE OFF").await.unwrap();
    admin.simple_query("maintenance on").await.unwrap(); // lowercase
    admin.simple_query("maintenance off").await.unwrap(); // lowercase

    // Test invalid commands should fail
    let result = admin.simple_query("MAINTENANCE").await;
    assert!(result.is_err());

    let result = admin.simple_query("MAINTENANCE INVALID").await;
    assert!(result.is_err());

    let result = admin.simple_query("MAINTENANCE ON EXTRA").await;
    assert!(result.is_err());
}

#[tokio::test]
#[serial]
async fn test_maintenance_mode_concurrent_operations() {
    let admin = admin_tokio().await;
    let conn = connection_failover().await;

    // Ensure maintenance mode is off initially
    admin.simple_query("MAINTENANCE OFF").await.unwrap();

    // Run concurrent operations
    let admin_clone = admin_tokio().await;
    let admin_task = tokio::spawn(async move {
        // Admin operations should work during maintenance mode
        admin_clone.simple_query("MAINTENANCE ON").await.unwrap();
        sleep(Duration::from_millis(25)).await;
        admin_clone.simple_query("SHOW VERSION").await.unwrap();
        admin_clone.simple_query("MAINTENANCE OFF").await.unwrap();
    });

    let client_task = tokio::spawn(async move {
        // Give admin time to turn on maintenance mode
        sleep(Duration::from_millis(10)).await;

        // This query will be blocked by maintenance mode, then unblocked
        conn.execute("SELECT 1").await.unwrap();
        conn.close().await;
    });

    // Wait for both tasks to complete
    tokio::try_join!(admin_task, client_task).unwrap();
}

#[tokio::test]
#[serial]
async fn test_maintenance_mode_transaction_behavior() {
    let admin = admin_tokio().await;
    let mut conns = connections_sqlx().await;

    let mut tx = vec![];
    for conn in &mut conns {
        tx.push(conn.begin().await.unwrap());
    }

    admin.simple_query("MAINTENANCE ON").await.unwrap();

    for mut tx in tx {
        tx.execute("SELECT 1").await.unwrap();
        tx.execute("SELECT 2").await.unwrap();
    }

    admin.simple_query("MAINTENANCE OFF").await.unwrap();

    for conn in conns {
        conn.close().await;
    }
}
