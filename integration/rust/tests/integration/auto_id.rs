//! Integration tests for auto_id injection of pgdog.unique_id()

use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Executor, Pool, Postgres};

const AUTO_ID_TABLE: &str = "auto_id_test";

async fn prepare_auto_id_table(pool: &Pool<Postgres>, admin: &Pool<Postgres>) {
    pool.execute(format!("DROP TABLE IF EXISTS {AUTO_ID_TABLE}").as_str())
        .await
        .unwrap();
    pool.execute(
        format!(
            "CREATE TABLE {AUTO_ID_TABLE} (
                id BIGINT PRIMARY KEY,
                customer_id BIGINT NOT NULL,
                name TEXT
            )"
        )
        .as_str(),
    )
    .await
    .unwrap();
    // Enable rewrites and reload config so schema is picked up
    admin.execute("SET rewrite_enabled TO true").await.unwrap();
    admin.execute("RELOAD").await.unwrap();
}

async fn cleanup_auto_id_table(pool: &Pool<Postgres>) {
    pool.execute(format!("DROP TABLE IF EXISTS {AUTO_ID_TABLE}").as_str())
        .await
        .ok();
    admin_sqlx().await.execute("RELOAD").await.unwrap();
}

#[tokio::test]
async fn test_auto_id_rewrite_mode_injects_id() {
    let admin = admin_sqlx().await;
    let mut pools = connections_sqlx().await;
    let pool = pools.swap_remove(1);

    prepare_auto_id_table(&pool, &admin).await;

    admin
        .execute("SET rewrite_primary_key TO rewrite")
        .await
        .unwrap();

    // Use unique values to avoid AST cache collision with other tests
    let result = sqlx::query(&format!(
        "INSERT INTO {AUTO_ID_TABLE} (customer_id, name) VALUES (100, 'rewrite_test')"
    ))
    .execute(&pool)
    .await;

    assert!(
        result.is_ok(),
        "INSERT should succeed with auto-injected id: {:?}",
        result.err()
    );

    let row: (i64, i64, String) = sqlx::query_as(&format!(
        "SELECT id, customer_id, name FROM {AUTO_ID_TABLE} WHERE customer_id = 100"
    ))
    .fetch_one(&pool)
    .await
    .expect("fetch inserted row");

    assert!(row.0 > 0, "auto-generated id should be positive");
    assert_eq!(row.1, 100, "customer_id should be 100");
    assert_eq!(row.2, "rewrite_test", "name should be 'rewrite_test'");

    cleanup_auto_id_table(&pool).await;
}

#[tokio::test]
async fn test_auto_id_error_mode_rejects_missing_pk() {
    let admin = admin_sqlx().await;
    let mut pools = connections_sqlx().await;
    let pool = pools.swap_remove(1);

    prepare_auto_id_table(&pool, &admin).await;

    // Verify table exists and has expected structure
    let check: (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{}'",
        AUTO_ID_TABLE
    ))
    .fetch_one(&pool)
    .await
    .expect("check table exists");
    eprintln!(
        "Table {} has {} columns in information_schema",
        AUTO_ID_TABLE, check.0
    );
    assert!(check.0 > 0, "Table should exist with columns");

    admin
        .execute("SET rewrite_primary_key TO error")
        .await
        .unwrap();

    // Use unique values to avoid AST cache collision with other tests
    let err = sqlx::query(&format!(
        "INSERT INTO {AUTO_ID_TABLE} (customer_id, name) VALUES (200, 'error_test')"
    ))
    .execute(&pool)
    .await
    .expect_err("INSERT without primary key should fail in error mode");

    let db_err = err
        .as_database_error()
        .expect("expected database error from proxy");
    assert!(
        db_err.message().contains("missing primary key"),
        "expected missing primary key error, got: {}",
        db_err.message()
    );

    cleanup_auto_id_table(&pool).await;
}

#[tokio::test]
async fn test_auto_id_ignore_mode_allows_missing_pk() {
    let admin = admin_sqlx().await;
    let mut pools = connections_sqlx().await;
    let pool = pools.swap_remove(1);

    prepare_auto_id_table(&pool, &admin).await;

    admin
        .execute("SET rewrite_primary_key TO ignore")
        .await
        .unwrap();

    // Use unique values to avoid AST cache collision with other tests
    // Insert without 'id' - should fail at DB level (NOT NULL from PRIMARY KEY)
    let err = sqlx::query(&format!(
        "INSERT INTO {AUTO_ID_TABLE} (customer_id, name) VALUES (300, 'ignore_test')"
    ))
    .execute(&pool)
    .await
    .expect_err("INSERT without primary key should fail at DB level");

    let db_err = err.as_database_error().expect("expected database error");
    assert!(
        db_err.message().contains("id") || db_err.message().contains("null"),
        "expected null violation or column error, got: {}",
        db_err.message()
    );

    cleanup_auto_id_table(&pool).await;
}

#[tokio::test]
async fn test_auto_id_with_explicit_pk_no_injection() {
    let admin = admin_sqlx().await;
    let mut pools = connections_sqlx().await;
    let pool = pools.swap_remove(1);

    prepare_auto_id_table(&pool, &admin).await;

    admin
        .execute("SET rewrite_primary_key TO rewrite")
        .await
        .unwrap();

    // Use unique values to avoid AST cache collision with other tests
    let result = sqlx::query(&format!(
        "INSERT INTO {AUTO_ID_TABLE} (id, customer_id, name) VALUES (42, 400, 'explicit_test')"
    ))
    .execute(&pool)
    .await;

    assert!(
        result.is_ok(),
        "INSERT with explicit id should succeed: {:?}",
        result.err()
    );

    let row: (i64, i64, String) = sqlx::query_as(&format!(
        "SELECT id, customer_id, name FROM {AUTO_ID_TABLE} WHERE id = 42"
    ))
    .fetch_one(&pool)
    .await
    .expect("fetch inserted row");

    assert_eq!(row.0, 42, "id should be our explicit value 42");
    assert_eq!(row.1, 400, "customer_id should be 400");
    assert_eq!(row.2, "explicit_test", "name should be 'explicit_test'");

    cleanup_auto_id_table(&pool).await;
}

#[tokio::test]
async fn test_auto_id_multi_row_insert() {
    let admin = admin_sqlx().await;
    let mut pools = connections_sqlx().await;
    let pool = pools.swap_remove(1);

    prepare_auto_id_table(&pool, &admin).await;

    admin
        .execute("SET rewrite_primary_key TO rewrite")
        .await
        .unwrap();
    admin
        .execute("SET rewrite_split_inserts TO rewrite")
        .await
        .unwrap();

    // Use unique values to avoid AST cache collision with other tests
    let result = sqlx::query(&format!(
        "INSERT INTO {AUTO_ID_TABLE} (customer_id, name) VALUES (500, 'multi_first'), (500, 'multi_second')"
    ))
    .execute(&pool)
    .await;

    assert!(
        result.is_ok(),
        "Multi-row INSERT should succeed: {:?}",
        result.err()
    );

    let rows: Vec<(i64, i64, String)> = sqlx::query_as(&format!(
        "SELECT id, customer_id, name FROM {AUTO_ID_TABLE} WHERE customer_id = 500 ORDER BY name"
    ))
    .fetch_all(&pool)
    .await
    .expect("fetch inserted rows");

    assert_eq!(rows.len(), 2, "expected 2 rows");
    assert!(rows[0].0 > 0, "first row id should be positive");
    assert!(rows[1].0 > 0, "second row id should be positive");
    assert_ne!(rows[0].0, rows[1].0, "each row should have unique id");
    assert_eq!(rows[0].2, "multi_first");
    assert_eq!(rows[1].2, "multi_second");

    cleanup_auto_id_table(&pool).await;
}

#[tokio::test]
async fn test_auto_id_default_replaced_with_unique_id() {
    let admin = admin_sqlx().await;
    let mut pools = connections_sqlx().await;
    let pool = pools.swap_remove(1);

    prepare_auto_id_table(&pool, &admin).await;

    admin
        .execute("SET rewrite_primary_key TO rewrite")
        .await
        .unwrap();

    // Insert with DEFAULT as primary key value - should be replaced with unique_id
    let result = sqlx::query(&format!(
        "INSERT INTO {AUTO_ID_TABLE} (id, customer_id, name) VALUES (DEFAULT, 600, 'default_test')"
    ))
    .execute(&pool)
    .await;

    assert!(
        result.is_ok(),
        "INSERT with DEFAULT id should succeed: {:?}",
        result.err()
    );

    let row: (i64, i64, String) = sqlx::query_as(&format!(
        "SELECT id, customer_id, name FROM {AUTO_ID_TABLE} WHERE customer_id = 600"
    ))
    .fetch_one(&pool)
    .await
    .expect("fetch inserted row");

    assert!(row.0 > 0, "auto-generated id should be positive");
    assert_eq!(row.1, 600, "customer_id should be 600");
    assert_eq!(row.2, "default_test", "name should be 'default_test'");

    cleanup_auto_id_table(&pool).await;
}
