use rust::setup::{admin_sqlx, connection_sqlx_direct, connections_sqlx};
use sqlx::{Executor, Row};

/// Test that PgDog gracefully handles connections terminated by administrator command.
/// When a connection is terminated via pg_terminate_backend(), PgDog should detect this
/// during healthcheck and fetch a fresh connection from the pool instead of giving
/// a dead connection to the client.
#[tokio::test]
async fn test_admin_termination_retry() {
    let conns = connections_sqlx().await;
    let pool = &conns[0];

    // Connect to PgDog admin database to configure healthcheck
    let pgdog_admin = admin_sqlx().await;

    // Set healthcheck_interval to 0 to force healthcheck on every connection checkout
    pgdog_admin
        .execute("SET healthcheck_interval TO 0")
        .await
        .unwrap();

    // First, establish a connection and verify it works
    let result = pool.fetch_one("SELECT 1 as num").await.unwrap();
    assert_eq!(result.get::<i32, _>("num"), 1);

    // Connect directly to Postgres to run admin commands
    let admin_pool = connection_sqlx_direct().await;

    // Query through PgDog to establish connections in the pool
    for _ in 0..5 {
        pool.execute("SELECT 1").await.unwrap();
    }

    // Give connections time to return to pool
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Get all backend PIDs connected to the database from the pooler
    // The pooler passes through the client's application_name, which is "sqlx"
    let pids: Vec<i32> = admin_pool
        .fetch_all(
            "SELECT pid FROM pg_stat_activity 
             WHERE datname = 'pgdog' 
             AND application_name = 'sqlx'
             AND state = 'idle'
             AND pid != pg_backend_pid()",
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get("pid"))
        .collect();

    assert!(
        !pids.is_empty(),
        "Should have at least one idle connection from the pooler"
    );

    // Terminate one of the backend connections
    let pid_to_terminate = pids[0];
    admin_pool
        .execute(format!("SELECT pg_terminate_backend({})", pid_to_terminate).as_str())
        .await
        .unwrap();

    // Give PgDog time to detect the termination on next healthcheck/checkout
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Now when we try to use the pool, PgDog should:
    // 1. Detect the terminated connection during healthcheck
    // 2. Discard it and fetch another connection
    // 3. Return a working connection to the client
    for i in 0..10 {
        let result = pool
            .fetch_one(format!("SELECT {} as num", i).as_str())
            .await;

        // The query should succeed - PgDog should transparently handle the terminated connection
        assert!(
            result.is_ok(),
            "Query should succeed even after admin termination: {:?}",
            result.err()
        );

        if let Ok(row) = result {
            assert_eq!(row.get::<i32, _>("num"), i);
        }
    }

    // Verify the pool is still healthy and can execute queries
    let final_result = pool.fetch_one("SELECT 42 as answer").await.unwrap();
    assert_eq!(final_result.get::<i32, _>("answer"), 42);

    // Reset PgDog settings to avoid affecting other tests
    pgdog_admin.execute("RELOAD").await.unwrap();
}
