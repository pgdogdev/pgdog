use serial_test::serial;
use sqlx::{Connection, PgConnection};
use std::process::{Child, Command};
use std::time::Duration;
use tokio_postgres::{NoTls, SimpleQueryMessage};

// Drop guard to ensure pgdog is always killed
struct PgdogGuard {
    child: Child,
}

impl Drop for PgdogGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

// Helper function to parse mirror stats from admin query
fn parse_mirror_stats(stats: &[SimpleQueryMessage]) -> (u64, u64, u64) {
    let mut total = 0u64;
    let mut mirrored = 0u64;
    let mut dropped = 0u64;

    for msg in stats {
        if let SimpleQueryMessage::Row(row) = msg {
            let metric = row.get(0).unwrap_or("");
            let value_str = row.get(1).unwrap_or("0");
            match metric {
                "requests_total" => total = value_str.parse().unwrap_or(0),
                "requests_mirrored" => mirrored = value_str.parse().unwrap_or(0),
                "requests_dropped" => dropped = value_str.parse().unwrap_or(0),
                _ => {}
            }
        }
    }

    (total, mirrored, dropped)
}

#[tokio::test]
#[serial]
async fn test_mirror_stats_with_unreachable_destination() {
    // Create a configuration with an unreachable mirror destination
    let config_file = "/tmp/mirror_stats_bug_test.toml";
    let users_file = "/tmp/mirror_stats_bug_test_users.toml";

    // Write configuration with unreachable mirror destination
    let config = r#"
[general]
openmetrics_port = 9090

# Source database
[[databases]]
name = "source_db"
host = "127.0.0.1"
database_name = "pgdog"

# Mirror destination - unreachable (wrong port)
[[databases]]
name = "mirror_db"
host = "127.0.0.1"
port = 9999  # Invalid port - connection will fail
database_name = "pgdog"

# Mirror configuration
[[mirroring]]
source = "source_db"
destination = "mirror_db"
exposure = 1.0  # Mirror everything

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
database = "mirror_db"
"#;

    std::fs::write(config_file, config).expect("Failed to write config");
    std::fs::write(users_file, users_config).expect("Failed to write users config");

    println!("Starting PgDog with unreachable mirror destination...");

    // Start PgDog (using debug build for trace logging)
    let _pgdog = PgdogGuard {
        child: Command::new("/Users/justin/Code/lev/pgdog/target/debug/pgdog")
            .arg("--config")
            .arg(config_file)
            .arg("--users")
            .arg(users_file)
            .spawn()
            .expect("Failed to start pgdog"),
    };

    // Give it time to start
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Set up source database
    let mut source_conn = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/pgdog")
        .await
        .expect("Failed to connect to source");

    // Clean up and create table
    let _ = sqlx::query("DROP TABLE IF EXISTS stats_test")
        .execute(&mut source_conn)
        .await;

    sqlx::query("CREATE TABLE stats_test (id INT PRIMARY KEY, data TEXT)")
        .execute(&mut source_conn)
        .await
        .expect("Failed to create table");

    // Connect to admin database to monitor stats
    let (admin_client, admin_connection) =
        tokio_postgres::connect("postgres://admin:pgdog@127.0.0.1:6432/admin", NoTls)
            .await
            .expect("Failed to connect to admin");

    tokio::spawn(async move {
        if let Err(e) = admin_connection.await {
            eprintln!("admin connection error: {}", e);
        }
    });

    // Get initial mirror stats
    let initial_stats = admin_client
        .simple_query("SHOW MIRROR_STATS")
        .await
        .expect("Failed to query initial mirror stats");

    let (initial_total, initial_mirrored, initial_dropped) = parse_mirror_stats(&initial_stats);

    println!(
        "Initial stats - Total: {}, Mirrored: {}, Dropped: {}",
        initial_total, initial_mirrored, initial_dropped
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

    // Execute a single transaction
    println!("Executing a single transaction through PgDog...");
    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("INSERT INTO stats_test (id, data) VALUES (1, 'Test data')")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();

    // Wait for mirror processing
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Get stats after transaction
    let final_stats = admin_client
        .simple_query("SHOW MIRROR_STATS")
        .await
        .expect("Failed to query final mirror stats");

    let (final_total, final_mirrored, final_dropped) = parse_mirror_stats(&final_stats);

    // Calculate differentials
    let stats_total = final_total - initial_total;
    let stats_mirrored = final_mirrored - initial_mirrored;
    let stats_dropped = final_dropped - initial_dropped;

    println!("\n=== Mirror Stats After Transaction ===");
    println!("Total requests: {}", stats_total);
    println!("Mirrored (claimed successful): {}", stats_mirrored);
    println!("Dropped: {}", stats_dropped);

    // Check for error counters
    let error_stats = admin_client
        .simple_query("SHOW MIRROR_STATS")
        .await
        .expect("Failed to query error stats");

    let mut errors_connection = 0u64;
    let mut errors_query = 0u64;
    let mut errors_timeout = 0u64;
    let mut errors_buffer_full = 0u64;

    for msg in &error_stats {
        if let SimpleQueryMessage::Row(row) = msg {
            let metric = row.get(0).unwrap_or("");
            let value_str = row.get(1).unwrap_or("0");
            match metric {
                "errors_connection" => errors_connection = value_str.parse().unwrap_or(0),
                "errors_query" => errors_query = value_str.parse().unwrap_or(0),
                "errors_timeout" => errors_timeout = value_str.parse().unwrap_or(0),
                "errors_buffer_full" => errors_buffer_full = value_str.parse().unwrap_or(0),
                _ => {}
            }
        }
    }

    println!("\n=== Error Counters ===");
    println!("Connection errors: {}", errors_connection);
    println!("Query errors: {}", errors_query);
    println!("Timeout errors: {}", errors_timeout);
    println!("Buffer full errors: {}", errors_buffer_full);

    // ASSERTIONS: What SHOULD happen
    println!("\n=== Expected Behavior ===");
    println!("- Total should be 3 (BEGIN, INSERT, COMMIT)");
    println!("- Mirrored should be 0 (destination unreachable)");
    println!("- Dropped should be 0 OR errors_connection should be > 0");
    println!("- Either the requests are dropped OR marked as connection errors");

    // The bug: requests are marked as "mirrored" even though they failed
    if stats_mirrored > 0 && errors_connection == 0 {
        println!(
            "\n🐛 BUG DETECTED: Requests marked as 'mirrored' despite unreachable destination!"
        );
        println!("   This is incorrect - failed mirror attempts should not count as successful.");
    }

    // What we expect: either dropped or error, but NOT successfully mirrored
    assert!(
        stats_mirrored == 0 || errors_connection > 0,
        "Bug: {} requests marked as 'mirrored' but destination was unreachable! Should be 0 mirrored or have connection errors.",
        stats_mirrored
    );
}
