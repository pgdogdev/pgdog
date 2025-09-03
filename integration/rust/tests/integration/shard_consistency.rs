use rust::setup::connections_sqlx;
use sqlx::{Executor, Row};

#[tokio::test]
async fn shard_consistency_validator() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    // Clean up any existing test tables
    sharded
        .execute("DROP TABLE IF EXISTS shard_test")
        .await
        .ok();

    // Create different table schemas on each shard to trigger validator errors
    // Shard 0: table with 2 columns (id, name)
    sharded
        .execute("/* pgdog_shard: 0 */ CREATE TABLE shard_test (id BIGINT PRIMARY KEY, name VARCHAR(100))")
        .await?;

    // Shard 1: table with 3 columns (id, name, extra) - different column count
    sharded
        .execute("/* pgdog_shard: 1 */ CREATE TABLE shard_test (id BIGINT PRIMARY KEY, name VARCHAR(100), extra TEXT)")
        .await?;

    // Insert some test data on each shard
    sharded
        .execute("/* pgdog_shard: 0 */ INSERT INTO shard_test (id, name) VALUES (1, 'shard0_row1'), (2, 'shard0_row2')")
        .await?;

    sharded
        .execute("/* pgdog_shard: 1 */ INSERT INTO shard_test (id, name, extra) VALUES (3, 'shard1_row1', 'extra_data'), (4, 'shard1_row2', 'more_data')")
        .await?;

    // Query that spans both shards should fail due to inconsistent column count
    let result = sharded
        .fetch_all("SELECT * FROM shard_test ORDER BY id")
        .await;

    // The query should fail with a shard consistency error
    assert!(
        result.is_err(),
        "Expected query to fail due to inconsistent schemas between shards"
    );

    let error = result.unwrap_err();
    let error_string = error.to_string();

    // Check that the error message indicates schema inconsistency
    assert!(
        error_string.contains("inconsistent row descriptions between shards"),
        "Expected error message to indicate row description inconsistency, got: {}",
        error_string
    );

    // Clean up
    sharded
        .execute("DROP TABLE IF EXISTS shard_test")
        .await
        .ok();

    Ok(())
}

#[tokio::test]
async fn shard_consistency_validator_column_names() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    // Clean up any existing test tables
    sharded
        .execute("DROP TABLE IF EXISTS shard_name_test")
        .await
        .ok();

    // Create tables with same column count but different column names
    // Shard 0: columns named (id, name)
    sharded
        .execute("/* pgdog_shard: 0 */ CREATE TABLE shard_name_test (id BIGINT PRIMARY KEY, name VARCHAR(100))")
        .await?;

    // Shard 1: columns named (id, username) - different column name
    sharded
        .execute("/* pgdog_shard: 1 */ CREATE TABLE shard_name_test (id BIGINT PRIMARY KEY, username VARCHAR(100))")
        .await?;

    // Insert test data
    sharded
        .execute("/* pgdog_shard: 0 */ INSERT INTO shard_name_test (id, name) VALUES (1, 'test1')")
        .await?;

    sharded
        .execute(
            "/* pgdog_shard: 1 */ INSERT INTO shard_name_test (id, username) VALUES (2, 'test2')",
        )
        .await?;

    // Query that spans both shards should fail due to inconsistent column names
    let result = sharded
        .fetch_all("SELECT * FROM shard_name_test ORDER BY id")
        .await;

    // The query should fail with a shard consistency error
    assert!(
        result.is_err(),
        "Expected query to fail due to inconsistent column names between shards"
    );

    let error = result.unwrap_err();
    let error_string = error.to_string();

    // Check that the error indicates column name inconsistency
    assert!(
        error_string.contains("inconsistent column names between shards"),
        "Expected error message to indicate column name inconsistency, got: {}",
        error_string
    );

    // Clean up
    sharded
        .execute("DROP TABLE IF EXISTS shard_name_test")
        .await
        .ok();

    Ok(())
}

#[tokio::test]
async fn shard_consistency_validator_success() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    // Clean up any existing test tables
    sharded
        .execute("DROP TABLE IF EXISTS shard_consistent_test")
        .await
        .ok();

    // Create identical table schemas on both shards
    sharded
        .execute("CREATE TABLE shard_consistent_test (id BIGINT PRIMARY KEY, name VARCHAR(100))")
        .await?;

    // Insert test data
    sharded
        .execute("/* pgdog_shard: 0 */ INSERT INTO shard_consistent_test (id, name) VALUES (1, 'shard0_data'), (2, 'shard0_more')")
        .await?;

    sharded
        .execute("/* pgdog_shard: 1 */ INSERT INTO shard_consistent_test (id, name) VALUES (3, 'shard1_data'), (4, 'shard1_more')")
        .await?;

    // Query that spans both shards should succeed with consistent schemas
    let rows = sharded
        .fetch_all("SELECT * FROM shard_consistent_test ORDER BY id")
        .await?;

    // Should get all 4 rows from both shards
    assert_eq!(rows.len(), 4);

    // Verify we got data from both shards
    let names: Vec<String> = rows
        .iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();
    assert!(names.contains(&"shard0_data".to_string()));
    assert!(names.contains(&"shard1_data".to_string()));

    // Clean up
    sharded
        .execute("DROP TABLE IF EXISTS shard_consistent_test")
        .await
        .ok();

    Ok(())
}

#[tokio::test]
async fn shard_consistency_data_row_validator_prepared_statement()
-> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    // Clean up any existing test tables
    sharded
        .execute("DROP TABLE IF EXISTS shard_datarow_test")
        .await
        .ok();

    // Create tables with same schema but we'll query them differently to trigger DataRow validation
    // Both tables have same structure so RowDescription will match initially
    sharded
        .execute("CREATE TABLE shard_datarow_test (id BIGINT PRIMARY KEY, name VARCHAR(100), extra TEXT DEFAULT 'default')")
        .await?;

    // Insert test data
    sharded
        .execute("/* pgdog_shard: 0 */ INSERT INTO shard_datarow_test (id, name) VALUES (1, 'test1'), (2, 'test2')")
        .await?;

    sharded
        .execute("/* pgdog_shard: 1 */ INSERT INTO shard_datarow_test (id, name, extra) VALUES (3, 'test3', 'extra3'), (4, 'test4', 'extra4')")
        .await?;

    // Use a prepared statement that selects different column sets from each shard
    // This creates a scenario where RowDescription might initially match, but DataRow counts differ
    // The key is to use conditional logic or different column selection per shard

    // Actually, let's create a simpler test - use views with different column counts
    sharded
        .execute("/* pgdog_shard: 0 */ CREATE VIEW datarow_view AS SELECT id, name FROM shard_datarow_test")
        .await?;

    sharded
        .execute("/* pgdog_shard: 1 */ CREATE VIEW datarow_view AS SELECT id, name, extra FROM shard_datarow_test") 
        .await?;

    // Now prepare a statement against the views
    let result = sharded
        .prepare("SELECT * FROM datarow_view WHERE id > $1 ORDER BY id")
        .await;

    // We want this test to specifically trigger DataRow validation, not RowDescription validation
    // If prepare fails due to RowDescription mismatch, we should fail the test
    let _stmt = result.expect("Prepare should succeed - we want to test DataRow validation, not RowDescription validation");

    // Now execute the prepared statement to trigger DataRow validation
    let execute_result = sqlx::query("SELECT * FROM datarow_view WHERE id > $1 ORDER BY id")
        .bind(0i64)
        .fetch_all(&sharded)
        .await;

    assert!(
        execute_result.is_err(),
        "Expected query to fail due to inconsistent DataRow column counts between shard views"
    );

    let error = execute_result.unwrap_err();
    let error_string = error.to_string();

    // Specifically check for DataRow count error (not RowDescription error)
    assert!(
        error_string.contains("inconsistent column count in data rows"),
        "Expected error message to indicate DataRow count inconsistency, got: {}. This test should specifically trigger DataRow validation, not RowDescription validation.",
        error_string
    );

    // Clean up
    sharded
        .execute("DROP VIEW IF EXISTS datarow_view")
        .await
        .ok();
    sharded
        .execute("DROP TABLE IF EXISTS shard_datarow_test")
        .await
        .ok();

    Ok(())
}
