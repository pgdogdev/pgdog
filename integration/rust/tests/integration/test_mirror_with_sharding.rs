use serial_test::serial;
use sqlx::{Connection, PgConnection};
use std::process::Command;
use std::time::Duration;
use tokio_postgres::NoTls;

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

    // Start PgDog
    let mut pgdog = Command::new("/Users/justin/Code/lev/pgdog/target/release/pgdog")
        .arg("--config")
        .arg("/tmp/mirror_test.toml")
        .arg("--users")
        .arg("/tmp/mirror_test_users.toml")
        .spawn()
        .expect("Failed to start pgdog");

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

    // Kill pgdog
    pgdog.kill().expect("Failed to kill pgdog");

    // Each shard should have 2 users
    assert_eq!(shard0_count.0, 2, "Shard 0 should have 2 users");
    assert_eq!(shard1_count.0, 2, "Shard 1 should have 2 users");

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
