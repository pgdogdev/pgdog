use serial_test::serial;
use sqlx::{Connection, PgConnection};
use std::collections::HashSet;
use std::process::{Child, Command};
use std::time::Duration;
use tokio_postgres::NoTls;

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

#[tokio::test]
#[serial]
async fn test_mirroring_to_sharded_cluster() {
    // Write the config file
    let config = r#"
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
exposure = 1.0

[admin]
password = "pgdog"
"#;

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

    // Write configs to temp files
    std::fs::write("/tmp/mirror_test.toml", config).expect("Failed to write config");
    std::fs::write("/tmp/mirror_test_users.toml", users_config)
        .expect("Failed to write users config");

    println!("Starting PgDog with mirroring to sharded cluster configuration...");

    // Start PgDog with automatic cleanup on drop
    let _pgdog = PgdogGuard {
        child: Command::new("/Users/justin/Code/lev/pgdog/target/release/pgdog")
            .arg("--config")
            .arg("/tmp/mirror_test.toml")
            .arg("--users")
            .arg("/tmp/mirror_test_users.toml")
            .spawn()
            .expect("Failed to start pgdog"),
    };

    // Give it time to start
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Connect directly to databases to set up tables
    let mut source_conn = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/pgdog")
        .await
        .expect("Failed to connect to source database");

    let mut shard0_conn = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_0")
        .await
        .expect("Failed to connect to shard_0");

    let mut shard1_conn = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_1")
        .await
        .expect("Failed to connect to shard_1");

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

    // Create table on source database (for inserts)
    sqlx::query("CREATE TABLE users (user_id BIGINT PRIMARY KEY, name TEXT)")
        .execute(&mut source_conn)
        .await
        .expect("Failed to create table on source");

    // Create table on both shards (for mirrored data)
    sqlx::query("CREATE TABLE users (user_id BIGINT PRIMARY KEY, name TEXT)")
        .execute(&mut shard0_conn)
        .await
        .expect("Failed to create table on shard_0");

    sqlx::query("CREATE TABLE users (user_id BIGINT PRIMARY KEY, name TEXT)")
        .execute(&mut shard1_conn)
        .await
        .expect("Failed to create table on shard_1");

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
    let shard0_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(&mut shard0_conn)
        .await
        .expect("Failed to query shard_0");

    let shard1_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(&mut shard1_conn)
        .await
        .expect("Failed to query shard_1");

    println!("Shard 0 has {} rows", shard0_count.0);
    println!("Shard 1 has {} rows", shard1_count.0);

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
    assert!(shard0_count.0 > 0, "Shard 0 should have at least 1 user");
    assert!(shard1_count.0 > 0, "Shard 1 should have at least 1 user");

    // Total should be 4 users
    assert_eq!(
        shard0_count.0 + shard1_count.0,
        4,
        "Total should be 4 users"
    );

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

    // Write the config file with exposure=0.5
    let config = r#"
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

# Mirror from source to sharded cluster with 50% exposure
[[mirroring]]
source = "source_db"
destination = "sharded_cluster"
exposure = 0.5

[admin]
password = "pgdog"
"#;

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

    // Write configs to temp files
    std::fs::write("/tmp/mirror_test_statistical.toml", config).expect("Failed to write config");
    std::fs::write("/tmp/mirror_test_statistical_users.toml", users_config)
        .expect("Failed to write users config");

    println!("Starting PgDog with statistical mirroring configuration (exposure=0.5)...");

    // Start PgDog with automatic cleanup on drop
    let _pgdog = PgdogGuard {
        child: Command::new("/Users/justin/Code/lev/pgdog/target/release/pgdog")
            .arg("--config")
            .arg("/tmp/mirror_test_statistical.toml")
            .arg("--users")
            .arg("/tmp/mirror_test_statistical_users.toml")
            .spawn()
            .expect("Failed to start pgdog"),
    };

    // Give it time to start
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Connect directly to databases to set up tables
    let mut source_conn = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/pgdog")
        .await
        .expect("Failed to connect to source database");

    let mut shard0_conn = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_0")
        .await
        .expect("Failed to connect to shard_0");

    let mut shard1_conn = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_1")
        .await
        .expect("Failed to connect to shard_1");

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

    // Create table on source database (for inserts)
    sqlx::query("CREATE TABLE users (user_id BIGINT PRIMARY KEY, name TEXT)")
        .execute(&mut source_conn)
        .await
        .expect("Failed to create table on source");

    // Create table on both shards (for mirrored data)
    sqlx::query("CREATE TABLE users (user_id BIGINT PRIMARY KEY, name TEXT)")
        .execute(&mut shard0_conn)
        .await
        .expect("Failed to create table on shard_0");

    sqlx::query("CREATE TABLE users (user_id BIGINT PRIMARY KEY, name TEXT)")
        .execute(&mut shard1_conn)
        .await
        .expect("Failed to create table on shard_1");

    // Connect to admin database to get initial mirror stats (must use simple protocol)
    let (admin_client, admin_connection) =
        tokio_postgres::connect("postgres://admin:pgdog@127.0.0.1:6432/admin", NoTls)
            .await
            .expect("Failed to connect to admin database");

    tokio::spawn(async move {
        if let Err(e) = admin_connection.await {
            eprintln!("admin connection error: {}", e);
        }
    });

    // Get initial mirror stats using simple_query
    let initial_stats = admin_client
        .simple_query("SHOW MIRROR_STATS")
        .await
        .expect("Failed to query initial mirror stats");

    let mut initial_total = 0u64;
    let mut initial_mirrored = 0u64;
    let mut initial_dropped = 0u64;
    
    // Parse the results - each row has metric name and value
    for msg in &initial_stats {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let metric = row.get(0).unwrap_or("");
            let value_str = row.get(1).unwrap_or("0");
            match metric {
                "requests_total" => initial_total = value_str.parse().unwrap_or(0),
                "requests_mirrored" => initial_mirrored = value_str.parse().unwrap_or(0),
                "requests_dropped" => initial_dropped = value_str.parse().unwrap_or(0),
                _ => {}
            }
        }
    }

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
    let shard0_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(&mut shard0_conn)
        .await
        .expect("Failed to query shard_0");

    let shard1_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(&mut shard1_conn)
        .await
        .expect("Failed to query shard_1");

    let total_mirrored = shard0_count.0 + shard1_count.0;
    println!(
        "Total mirrored: {} (shard0={}, shard1={})",
        total_mirrored, shard0_count.0, shard1_count.0
    );

    // Statistical assertion with tolerance
    assert!(
        total_mirrored >= 35 && total_mirrored <= 65,
        "Expected ~50 rows mirrored (±15), got {}",
        total_mirrored
    );

    // Verify roughly even distribution between shards (with wider tolerance)
    let shard_diff = (shard0_count.0 - shard1_count.0).abs();
    let max_acceptable_diff = (total_mirrored as f64 * 0.4) as i64;
    assert!(
        shard_diff <= max_acceptable_diff,
        "Shard distribution too uneven: shard0={}, shard1={}, diff={}",
        shard0_count.0,
        shard1_count.0,
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

    // Parse the final stats from the result
    let mut final_total = 0u64;
    let mut final_mirrored = 0u64;
    let mut final_dropped = 0u64;
    
    for msg in &final_stats {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let metric = row.get(0).unwrap_or("");
            let value_str = row.get(1).unwrap_or("0");
            match metric {
                "requests_total" => final_total = value_str.parse().unwrap_or(0),
                "requests_mirrored" => final_mirrored = value_str.parse().unwrap_or(0),
                "requests_dropped" => final_dropped = value_str.parse().unwrap_or(0),
                _ => {}
            }
        }
    }

    // Calculate differentials
    let stats_total = final_total - initial_total;
    let stats_mirrored = final_mirrored - initial_mirrored;
    let stats_dropped = final_dropped - initial_dropped;

    println!(
        "MirrorStats Differential - Total: {}, Mirrored: {}, Dropped: {}",
        stats_total, stats_mirrored, stats_dropped
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
    
    // With 50% exposure and hash-based selection, the stats_mirrored should be close to total_mirrored
    let tolerance = 10; // Allow some variance due to transaction bundling
    assert!(
        (stats_mirrored as i64 - total_mirrored as i64).abs() <= tolerance,
        "Stats mirrored ({}) should be close to actual mirrored rows ({}) ±{}",
        stats_mirrored, total_mirrored, tolerance
    );
    
    // Dropped should be 0 or very low (only actual errors/overflow, not exposure sampling)
    assert!(
        stats_dropped <= 10,
        "Dropped count should be low (only errors/overflow), got {}",
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
        shard0_count.0, shard1_count.0
    );
}
