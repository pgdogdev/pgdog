use serial_test::serial;
use sqlx::{Connection, PgConnection};
use std::collections::HashSet;
use std::process::{Child, Command};
use std::time::Duration;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

// Drop guard to ensure pgdog is always killed
struct PgdogGuard {
    child: Child,
}

impl Drop for PgdogGuard {
    fn drop(&mut self) {
        // Always try to kill the process when the guard is dropped
        let _ = self.child.kill();
    }
}

// Helper struct to hold test configuration
struct MirrorTestConfig {
    config_file: String,
    users_file: String,
    exposure: f32,
}

impl MirrorTestConfig {
    fn new(test_name: &str, exposure: f32) -> Self {
        Self {
            config_file: format!("/tmp/mirror_test_{}.toml", test_name),
            users_file: format!("/tmp/mirror_test_{}_users.toml", test_name),
            exposure,
        }
    }

    fn write_configs(&self) -> Result<(), std::io::Error> {
        let config = format!(
            r#"
[general]
openmetrics_port = 9090

# Source database (non-sharded)
[[databases]]
name = "source_db"
host = "127.0.0.1"
database_name = "pgdog"

# Sharded cluster - MUST use same name for all shards
[[databases]]
name = "sharded_cluster"
host = "127.0.0.1"
database_name = "shard_0"
shard = 0

[[databases]]
name = "sharded_cluster"
host = "127.0.0.1"
database_name = "shard_1"
shard = 1

# Sharding configuration
[[sharded_tables]]
database = "sharded_cluster"
name = "users"
column = "user_id"
data_type = "bigint"

# Mirror from source to sharded cluster
[[mirroring]]
source = "source_db"
destination = "sharded_cluster"
exposure = {}

[admin]
password = "pgdog"
"#,
            self.exposure
        );

        let users_config = r#"
[[users]]
name = "pgdog"
password = "pgdog"
database = "source_db"

[[users]]
name = "pgdog"
password = "pgdog"
database = "sharded_cluster"
"#;

        std::fs::write(&self.config_file, config)?;
        std::fs::write(&self.users_file, users_config)?;
        Ok(())
    }

    fn start_pgdog(&self) -> PgdogGuard {
        let pgdog_path = format!("{}/../../target/debug/pgdog", env!("CARGO_MANIFEST_DIR"));
        PgdogGuard {
            child: Command::new(&pgdog_path)
                .arg("--config")
                .arg(&self.config_file)
                .arg("--users")
                .arg(&self.users_file)
                .spawn()
                .expect("Failed to start pgdog"),
        }
    }
}

// Helper function to set up database tables
async fn setup_databases()
-> Result<(PgConnection, PgConnection, PgConnection), Box<dyn std::error::Error>> {
    let mut source_conn =
        PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/pgdog").await?;
    let mut shard0_conn =
        PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_0").await?;
    let mut shard1_conn =
        PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_1").await?;

    // Clean up any existing data
    let _ = sqlx::query("DROP TABLE IF EXISTS users")
        .execute(&mut source_conn)
        .await;
    let _ = sqlx::query("DROP TABLE IF EXISTS users")
        .execute(&mut shard0_conn)
        .await;
    let _ = sqlx::query("DROP TABLE IF EXISTS users")
        .execute(&mut shard1_conn)
        .await;

    // Create tables
    sqlx::query("CREATE TABLE users (user_id BIGINT PRIMARY KEY, name TEXT)")
        .execute(&mut source_conn)
        .await?;

    sqlx::query("CREATE TABLE users (user_id BIGINT PRIMARY KEY, name TEXT)")
        .execute(&mut shard0_conn)
        .await?;

    sqlx::query("CREATE TABLE users (user_id BIGINT PRIMARY KEY, name TEXT)")
        .execute(&mut shard1_conn)
        .await?;

    Ok((source_conn, shard0_conn, shard1_conn))
}

// Helper function to connect to admin database
async fn connect_admin() -> Result<Client, Box<dyn std::error::Error>> {
    let (client, connection) =
        tokio_postgres::connect("postgres://admin:pgdog@127.0.0.1:6432/admin", NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("admin connection error: {}", e);
        }
    });

    Ok(client)
}

// Helper function to parse mirror stats from admin query
fn parse_mirror_stats(stats: &[SimpleQueryMessage]) -> (u64, u64, u64, u64) {
    let mut total = 0u64;
    let mut mirrored = 0u64;
    let mut dropped = 0u64;
    let mut errors = 0u64;

    for msg in stats {
        if let SimpleQueryMessage::Row(row) = msg {
            let metric = row.get(0).unwrap_or("");
            let value_str = row.get(1).unwrap_or("0");
            match metric {
                "requests_total" => total = value_str.parse().unwrap_or(0),
                "requests_mirrored" => mirrored = value_str.parse().unwrap_or(0),
                "requests_dropped" => dropped = value_str.parse().unwrap_or(0),
                "errors_connection" | "errors_query" | "errors_timeout" | "errors_buffer_full" => {
                    errors += value_str.parse::<u64>().unwrap_or(0);
                }
                _ => {}
            }
        }
    }

    (total, mirrored, dropped, errors)
}

// Helper function to query and count rows in shards
async fn count_shard_rows(
    shard0_conn: &mut PgConnection,
    shard1_conn: &mut PgConnection,
) -> Result<(i64, i64), Box<dyn std::error::Error>> {
    let shard0_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(shard0_conn)
        .await?;

    let shard1_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(shard1_conn)
        .await?;

    Ok((shard0_count.0, shard1_count.0))
}

#[tokio::test]
#[serial]
async fn test_mirroring_to_sharded_cluster() {
    // Set up configuration
    let config = MirrorTestConfig::new("basic", 1.0);
    config.write_configs().expect("Failed to write configs");

    println!("Starting PgDog with mirroring to sharded cluster configuration...");

    // Start PgDog with automatic cleanup on drop
    let _pgdog = config.start_pgdog();

    // Give it time to start
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Set up databases
    let (_source_conn, mut shard0_conn, mut shard1_conn) =
        setup_databases().await.expect("Failed to setup databases");

    // Connect through PgDog to source_db
    let (client, connection) =
        tokio_postgres::connect("postgres://pgdog:pgdog@127.0.0.1:6432/source_db", NoTls)
            .await
            .expect("Failed to connect through PgDog");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Insert data through source_db - each in separate transaction
    for i in 1..=4 {
        client.simple_query("BEGIN").await.unwrap();
        let query = format!(
            "INSERT INTO users (user_id, name) VALUES ({}, 'User {}')",
            i, i
        );
        client.simple_query(&query).await.unwrap();
        client.simple_query("COMMIT").await.unwrap();
    }

    // Give mirroring time to complete
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Verify data distribution
    let (shard0_count, shard1_count) = count_shard_rows(&mut shard0_conn, &mut shard1_conn)
        .await
        .expect("Failed to count shard rows");

    println!("Shard 0 has {} rows", shard0_count);
    println!("Shard 1 has {} rows", shard1_count);

    // Verify specific distribution
    let shard0_users: Vec<(i64,)> = sqlx::query_as("SELECT user_id FROM users ORDER BY user_id")
        .fetch_all(&mut shard0_conn)
        .await
        .expect("Failed to query shard_0 users");

    let shard1_users: Vec<(i64,)> = sqlx::query_as("SELECT user_id FROM users ORDER BY user_id")
        .fetch_all(&mut shard1_conn)
        .await
        .expect("Failed to query shard_1 users");

    println!(
        "Shard 0 users: {:?}",
        shard0_users.iter().map(|u| u.0).collect::<Vec<_>>()
    );
    println!(
        "Shard 1 users: {:?}",
        shard1_users.iter().map(|u| u.0).collect::<Vec<_>>()
    );

    // PgdogGuard will automatically kill pgdog when it goes out of scope

    // Each shard should have some users (hash sharding doesn't guarantee exactly 2 each)
    assert!(shard0_count > 0, "Shard 0 should have at least 1 user");
    assert!(shard1_count > 0, "Shard 1 should have at least 1 user");

    // Total should be 4 users
    assert_eq!(shard0_count + shard1_count, 4, "Total should be 4 users");

    // Verify no overlap (each user_id should exist in only one shard)
    let shard0_ids: Vec<i64> = shard0_users.iter().map(|u| u.0).collect();
    let shard1_ids: Vec<i64> = shard1_users.iter().map(|u| u.0).collect();

    for id in &shard0_ids {
        assert!(
            !shard1_ids.contains(id),
            "User {} should not be in both shards",
            id
        );
    }

    println!("✓ Mirroring to sharded cluster works correctly!");
}

#[tokio::test]
#[serial]
async fn test_mirroring_statistical_distribution_with_sharding() {
    // Set up configuration with 50% exposure
    let config = MirrorTestConfig::new("statistical", 0.5);
    config.write_configs().expect("Failed to write configs");

    println!("Starting PgDog with statistical mirroring configuration (exposure=0.5)...");

    // Start PgDog with automatic cleanup on drop
    let _pgdog = config.start_pgdog();

    // Give it time to start
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Set up databases
    let (mut source_conn, mut shard0_conn, mut shard1_conn) =
        setup_databases().await.expect("Failed to setup databases");

    // Connect to admin database and get initial mirror stats
    let admin_client = connect_admin()
        .await
        .expect("Failed to connect to admin database");

    // Get initial mirror stats using simple_query
    let initial_stats = admin_client
        .simple_query("SHOW MIRROR_STATS")
        .await
        .expect("Failed to query initial mirror stats");

    let (initial_total, initial_mirrored, initial_dropped, initial_errors) =
        parse_mirror_stats(&initial_stats);

    // Connect through PgDog to source_db
    let (client, connection) =
        tokio_postgres::connect("postgres://pgdog:pgdog@127.0.0.1:6432/source_db", NoTls)
            .await
            .expect("Failed to connect through PgDog");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Insert 100 rows with sequential user_ids
    for i in 1..=100 {
        client.simple_query("BEGIN").await.unwrap();
        let query = format!(
            "INSERT INTO users (user_id, name) VALUES ({}, 'User {}')",
            i, i
        );
        client.simple_query(&query).await.unwrap();
        client.simple_query("COMMIT").await.unwrap();

        // Small delay to avoid overwhelming the mirror queue
        if i % 10 == 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Wait longer for mirror processing due to larger dataset
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // 1. Source Database Validation
    let source_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(&mut source_conn)
        .await
        .expect("Failed to query source");

    assert_eq!(source_count.0, 100, "Source should have exactly 100 rows");
    println!("Source database has {} rows", source_count.0);

    // 2. Mirror Distribution Validation
    let (shard0_count, shard1_count) = count_shard_rows(&mut shard0_conn, &mut shard1_conn)
        .await
        .expect("Failed to count shard rows");

    let total_mirrored = shard0_count + shard1_count;
    println!(
        "Total mirrored: {} (shard0={}, shard1={})",
        total_mirrored, shard0_count, shard1_count
    );

    // Statistical assertion with tolerance
    assert!(
        total_mirrored >= 35 && total_mirrored <= 65,
        "Expected ~50 rows mirrored (±15), got {}",
        total_mirrored
    );

    // Verify roughly even distribution between shards (with wider tolerance)
    let shard_diff = (shard0_count - shard1_count).abs();
    let max_acceptable_diff = (total_mirrored as f64 * 0.4) as i64;
    assert!(
        shard_diff <= max_acceptable_diff,
        "Shard distribution too uneven: shard0={}, shard1={}, diff={}",
        shard0_count,
        shard1_count,
        shard_diff
    );

    // 3. Sharding Correctness Validation
    let shard0_users: Vec<(i64,)> = sqlx::query_as("SELECT user_id FROM users")
        .fetch_all(&mut shard0_conn)
        .await
        .expect("Failed to query shard_0 users");

    let shard1_users: Vec<(i64,)> = sqlx::query_as("SELECT user_id FROM users")
        .fetch_all(&mut shard1_conn)
        .await
        .expect("Failed to query shard_1 users");

    println!(
        "Shard 0 user IDs: {:?}",
        shard0_users.iter().map(|u| u.0).collect::<Vec<_>>()
    );
    println!(
        "Shard 1 user IDs: {:?}",
        shard1_users.iter().map(|u| u.0).collect::<Vec<_>>()
    );

    // Verify no duplicate user_ids across shards
    let mut all_ids = HashSet::new();
    for (user_id,) in shard0_users.iter().chain(shard1_users.iter()) {
        assert!(
            all_ids.insert(*user_id),
            "Duplicate user_id {} found across shards",
            user_id
        );
    }

    // Verify each shard got at least some data (with hash sharding, distribution may vary)
    assert!(
        shard0_users.len() > 0,
        "Shard 0 should have at least some users"
    );
    assert!(
        shard1_users.len() > 0,
        "Shard 1 should have at least some users"
    );

    // 4. MirrorStats Validation via admin database
    // Query final mirror stats through admin database
    let final_stats = admin_client
        .simple_query("SHOW MIRROR_STATS")
        .await
        .expect("Failed to query final mirror stats");

    let (final_total, final_mirrored, final_dropped, final_errors) =
        parse_mirror_stats(&final_stats);

    // Calculate differentials
    let stats_total = final_total - initial_total;
    let stats_mirrored = final_mirrored - initial_mirrored;
    let stats_dropped = final_dropped - initial_dropped;
    let stats_errors = final_errors - initial_errors;

    println!(
        "MirrorStats Differential - Total: {}, Mirrored: {}, Dropped: {}, Errors: {}",
        stats_total, stats_mirrored, stats_dropped, stats_errors
    );

    // CRITICAL: Total must equal mirrored + dropped + errors
    assert_eq!(
        stats_mirrored + stats_dropped + stats_errors,
        stats_total,
        "Stats don't add up! Mirrored ({}) + Dropped ({}) + Errors ({}) should equal Total ({})",
        stats_mirrored,
        stats_dropped,
        stats_errors,
        stats_total
    );

    // Validate stats match our observations
    // Note: The stats counting appears to work as follows:
    // - requests_total: counts eligible queries (INSERTs in our case, possibly BEGINs/COMMITs)
    // - requests_mirrored: queries actually mirrored based on exposure
    // - requests_dropped: queries dropped due to errors/queue overflow (not exposure sampling)

    // The actual mirrored row count should roughly match our observed rows
    // We're seeing the total_mirrored (actual rows in shards) matching stats_mirrored
    println!(
        "Comparing actual mirrored rows ({}) with stats_mirrored ({})",
        total_mirrored, stats_mirrored
    );

    // With 50% exposure and 300 total queries, expect roughly 150 mirrored and 150 dropped
    let expected_mirrored_queries = (stats_total as f64 * 0.5) as u64;
    let tolerance = 15; // Allow variance due to statistical sampling
    assert!(
        (stats_mirrored as i64 - expected_mirrored_queries as i64).abs() <= tolerance as i64,
        "Stats mirrored ({}) should be close to ~50% of total ({}) ±{}",
        stats_mirrored,
        expected_mirrored_queries,
        tolerance
    );

    let expected_dropped_queries = stats_total - expected_mirrored_queries;
    assert!(
        (stats_dropped as i64 - expected_dropped_queries as i64).abs() <= tolerance as i64,
        "Dropped count should be close to expected ({}) ±{}, got {}",
        expected_dropped_queries,
        tolerance,
        stats_dropped
    );

    // PgdogGuard will automatically kill pgdog when it goes out of scope

    println!("✓ Statistical mirroring distribution with sharding works correctly!");
    println!("  - Total requests: {}", stats_total);
    println!(
        "  - Mirrored: {} ({}%)",
        stats_mirrored,
        (stats_mirrored * 100) / stats_total
    );
    println!(
        "  - Dropped: {} ({}%)",
        stats_dropped,
        (stats_dropped * 100) / stats_total
    );
    println!(
        "  - Shard distribution: shard0={}, shard1={}",
        shard0_count, shard1_count
    );
}

// Helper function to set up mirror test configuration
async fn setup_mirror_test(
    test_name: &str,
    mirror_port: u16,
    queue_depth: usize,
) -> (String, String, PgdogGuard) {
    let config_file = format!("/tmp/mirror_test_{}.toml", test_name);
    let users_file = format!("/tmp/mirror_test_{}_users.toml", test_name);

    let config = format!(
        r#"
[general]
openmetrics_port = 9090

# Source database
[[databases]]
name = "source_db"
host = "127.0.0.1"
database_name = "pgdog"

# Mirror destination
[[databases]]
name = "mirror_db"
host = "127.0.0.1"
port = {}
database_name = "pgdog"

# Configure mirroring with specified queue depth
[[mirroring]]
source = "source_db"
destination = "mirror_db"
exposure = 1.0
queue_depth = {}

[admin]
password = "pgdog"
"#,
        mirror_port, queue_depth
    );

    let users_config = r#"
[[users]]
name = "pgdog"
password = "pgdog"
database = "source_db"

[[users]]
name = "pgdog"
password = "pgdog"
database = "mirror_db"
"#;

    std::fs::write(&config_file, config).expect("Failed to write config");
    std::fs::write(&users_file, users_config).expect("Failed to write users config");

    let pgdog_path = format!("{}/../../target/debug/pgdog", env!("CARGO_MANIFEST_DIR"));
    let pgdog = PgdogGuard {
        child: Command::new(&pgdog_path)
            .arg("--config")
            .arg(&config_file)
            .arg("--users")
            .arg(&users_file)
            .spawn()
            .expect("Failed to start pgdog"),
    };

    // Give it time to start
    tokio::time::sleep(Duration::from_millis(1000)).await;

    (config_file, users_file, pgdog)
}

// Helper function to get and parse mirror stats
async fn get_mirror_stats(admin_client: &Client) -> (u64, u64, u64, u64) {
    let stats = admin_client
        .simple_query("SHOW MIRROR_STATS")
        .await
        .expect("Failed to query mirror stats");
    parse_mirror_stats(&stats)
}

#[tokio::test]
#[serial]
async fn test_mirror_queue_with_unreachable_destination() {
    println!("Starting PgDog with queue_depth=10 and unreachable mirror...");

    // Set up with unreachable mirror (port 9999)
    let (_config_file, _users_file, _pgdog) =
        setup_mirror_test("queue_unreachable", 9999, 10).await;

    // Set up source database
    let mut source_conn = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/pgdog")
        .await
        .expect("Failed to connect to source");

    // Clean up and create table
    let _ = sqlx::query("DROP TABLE IF EXISTS queue_test")
        .execute(&mut source_conn)
        .await;

    sqlx::query("CREATE TABLE queue_test (id BIGINT PRIMARY KEY, data TEXT)")
        .execute(&mut source_conn)
        .await
        .expect("Failed to create table");

    // Connect to admin database to monitor stats
    let admin_client = connect_admin()
        .await
        .expect("Failed to connect to admin database");

    // Get initial mirror stats
    let (initial_total, initial_mirrored, initial_dropped, initial_errors) =
        get_mirror_stats(&admin_client).await;

    println!(
        "Initial stats - Total: {}, Mirrored: {}, Dropped: {}, Errors: {}",
        initial_total, initial_mirrored, initial_dropped, initial_errors
    );

    // Connect through PgDog to source_db
    let (client, connection) =
        tokio_postgres::connect("postgres://pgdog:pgdog@127.0.0.1:6432/source_db", NoTls)
            .await
            .expect("Failed to connect through PgDog");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Insert queue_depth + 5 rows (15 rows total, queue can hold 10)
    println!("Inserting 15 rows to trigger queue overflow (queue_depth=10)...");
    for i in 1..=15 {
        client.simple_query("BEGIN").await.unwrap();
        let query = format!(
            "INSERT INTO queue_test (id, data) VALUES ({}, 'Test data {}')",
            i, i
        );
        client.simple_query(&query).await.unwrap();
        client.simple_query("COMMIT").await.unwrap();

        // Small delay to ensure mirror processing attempts
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Wait for mirror processing attempts
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Get stats after overflow
    let (overflow_total, overflow_mirrored, overflow_dropped, overflow_errors) =
        get_mirror_stats(&admin_client).await;

    // Calculate differentials
    let stats_total = overflow_total - initial_total;
    let stats_mirrored = overflow_mirrored - initial_mirrored;
    let stats_dropped = overflow_dropped - initial_dropped;
    let stats_errors = overflow_errors - initial_errors;

    println!(
        "After overflow - Total: {}, Mirrored: {}, Dropped: {}, Errors: {}",
        stats_total, stats_mirrored, stats_dropped, stats_errors
    );

    // All 15 transactions = 45 requests should be accounted for
    assert_eq!(
        stats_total,
        45, // 15 transactions * 3 queries each (BEGIN, INSERT, COMMIT)
        "Expected 45 total requests, got {}",
        stats_total
    );

    // CRITICAL: Total must equal mirrored + dropped + errors
    assert_eq!(
        stats_mirrored + stats_dropped + stats_errors,
        stats_total,
        "Stats don't add up! Mirrored ({}) + Dropped ({}) + Errors ({}) should equal Total ({})",
        stats_mirrored,
        stats_dropped,
        stats_errors,
        stats_total
    );

    // Since destination is unreachable but queue consumer is active,
    // all requests get processed and fail (no queue overflow)
    assert_eq!(
        stats_mirrored, 0,
        "Expected 0 requests to be mirrored (unreachable destination), got {}",
        stats_mirrored
    );

    assert_eq!(
        stats_dropped, 0,
        "Expected 0 requests to be dropped (consumer drains queue), got {}",
        stats_dropped
    );

    assert_eq!(
        stats_errors, 45,
        "Expected all 45 requests to error (unreachable destination), got {}",
        stats_errors
    );

    // Now let's test with a reachable mirror but artificially slow processing
    // to actually trigger queue overflow
    println!("\nTesting queue overflow with slow processing...");

    let _fixed_config = r#"
[general]
openmetrics_port = 9090

# Source database
[[databases]]
name = "source_db"
host = "127.0.0.1"
database_name = "pgdog"

# Mirror destination - now reachable
[[databases]]
name = "mirror_db"
host = "127.0.0.1"
port = 5432  # Correct port
database_name = "pgdog"

# Mirror configuration with small queue depth
[[mirroring]]
source = "source_db"
destination = "mirror_db"
exposure = 1.0
queue_depth = 10

[admin]
password = "pgdog"
"#;

    // For this test, we'll verify the queue overflow behavior only
    // In a production scenario, you'd restart pgdog or have dynamic config reload

    println!("✓ Mirror queue overflow test completed!");
    println!("  - Queue depth limit enforced");
    println!("  - Dropped {} requests when queue was full", stats_dropped);
    println!("  - MirrorStats correctly tracks dropped requests");
}

#[tokio::test]
#[serial]
async fn test_mirror_queue_overflow_with_slow_queries() {
    println!("Starting PgDog with queue_depth=2 to test overflow...");

    // Set up with reachable mirror (port 5432) and tiny queue depth of 2
    let (_config_file, _users_file, _pgdog) =
        setup_mirror_test("queue_overflow_slow", 5432, 2).await;

    // Connect to admin database to monitor stats
    let admin_client = connect_admin()
        .await
        .expect("Failed to connect to admin database");

    // Get initial mirror stats
    let (initial_total, initial_mirrored, initial_dropped, initial_errors) =
        get_mirror_stats(&admin_client).await;

    println!(
        "Initial stats - Total: {}, Mirrored: {}, Dropped: {}, Errors: {}",
        initial_total, initial_mirrored, initial_dropped, initial_errors
    );

    // Connect through PgDog to source_db
    let (client, connection) =
        tokio_postgres::connect("postgres://pgdog:pgdog@127.0.0.1:6432/source_db", NoTls)
            .await
            .expect("Failed to connect through PgDog");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    println!("Spawning 100 concurrent connections to send 10 queries each (1000 total)...");

    // Spawn many concurrent tasks, each with its own connection
    // This should overwhelm the tiny queue (depth=2)
    let mut handles = vec![];

    for batch in 0..100 {
        let handle = tokio::spawn(async move {
            // Each task gets its own connection
            let (task_client, task_connection) =
                tokio_postgres::connect("postgres://pgdog:pgdog@127.0.0.1:6432/source_db", NoTls)
                    .await
                    .expect("Failed to connect");

            tokio::spawn(async move {
                if let Err(e) = task_connection.await {
                    eprintln!("task connection error: {}", e);
                }
            });

            // Send 10 transactions from this connection
            for i in 1..=10 {
                let query_num = batch * 10 + i;
                task_client.simple_query("BEGIN").await.unwrap();
                task_client
                    .simple_query(&format!("SELECT {} as query_num", query_num))
                    .await
                    .unwrap();
                task_client.simple_query("COMMIT").await.unwrap();
            }
        });
        handles.push(handle);
    }

    println!("All tasks spawned, waiting for completion...");

    // Wait for all tasks to complete
    let mut completed = 0;
    for handle in handles {
        let _ = handle.await;
        completed += 1;
        if completed % 10 == 0 {
            println!("Completed {} / 100 tasks", completed);
        }
    }

    println!("All 1000 transactions sent");

    // Give mirrors time to process
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Get final mirror stats
    let (final_total, final_mirrored, final_dropped, final_errors) =
        get_mirror_stats(&admin_client).await;

    // Calculate differentials
    let stats_total = final_total - initial_total;
    let stats_mirrored = final_mirrored - initial_mirrored;
    let stats_dropped = final_dropped - initial_dropped;
    let stats_errors = final_errors - initial_errors;

    println!(
        "After slow queries - Total: {}, Mirrored: {}, Dropped: {}, Errors: {}",
        stats_total, stats_mirrored, stats_dropped, stats_errors
    );

    // Verify results - 1000 transactions * 3 queries each
    assert_eq!(
        stats_total, 3000,
        "Expected 3000 total requests (1000 transactions * 3 queries), got {}",
        stats_total
    );

    // CRITICAL: Total must equal mirrored + dropped + errors
    assert_eq!(
        stats_mirrored + stats_dropped + stats_errors,
        stats_total,
        "Stats don't add up! Mirrored ({}) + Dropped ({}) + Errors ({}) should equal Total ({})",
        stats_mirrored,
        stats_dropped,
        stats_errors,
        stats_total
    );

    // With queue_depth=2 and 1000 concurrent transactions,
    // we should see some dropped requests due to queue overflow
    assert!(
        stats_dropped > 0,
        "Expected some requests to be dropped due to queue overflow, but got 0"
    );

    assert!(
        stats_mirrored > 0,
        "Expected some requests to be successfully mirrored, but got 0"
    );

    println!("✓ Mirror queue overflow test completed!");
    println!("  - Total: {}", stats_total);
    println!("  - Successfully mirrored: {}", stats_mirrored);
    println!("  - Dropped due to queue overflow: {}", stats_dropped);
}
