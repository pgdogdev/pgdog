use crate::{expect_message, net::CommandComplete};

use super::prelude::*;

/// Get the pool IDs for all pools in the cluster.
fn get_pool_ids(engine: &mut QueryEngine) -> Vec<u64> {
    let cluster = engine.backend().cluster().unwrap();
    cluster
        .shards()
        .iter()
        .flat_map(|shard| shard.pools().into_iter().map(|pool| pool.id()))
        .collect()
}

/// Helper to run DDL in a transaction and verify schema_changed is set and reload occurs.
async fn assert_ddl_sets_schema_changed(ddl: &str, expected_cmd: &str, cleanup: Option<&str>) {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // Capture pool IDs before DDL
    let pool_ids_before = get_pool_ids(&mut test_client.engine);

    // Verify schema_changed starts as false
    assert!(
        !test_client.engine.router().schema_changed(),
        "schema_changed should be false before DDL"
    );

    // Start transaction to observe schema_changed before it's cleared
    test_client.send_simple(Query::new("BEGIN")).await;
    test_client.read_until('Z').await.unwrap();

    // Execute DDL
    test_client.send_simple(Query::new(ddl)).await;
    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        expected_cmd
    );
    let _rfq = test_client.read().await; // ReadyForQuery

    // schema_changed should be true
    assert!(
        test_client.engine.router().schema_changed(),
        "schema_changed should be true after {expected_cmd}"
    );

    // COMMIT to trigger reload
    test_client.send_simple(Query::new("COMMIT")).await;
    test_client.read_until('Z').await.unwrap();

    // Drop client to release resources
    drop(test_client);

    // Create new client to verify reload happened
    let mut new_client = TestClient::new_sharded(Parameters::default()).await;
    let pool_ids_after = get_pool_ids(&mut new_client.engine);

    assert!(
        pool_ids_after
            .iter()
            .all(|id| !pool_ids_before.contains(id)),
        "pools should have reloaded after {expected_cmd}: before={:?}, after={:?}",
        pool_ids_before,
        pool_ids_after
    );

    // Cleanup if needed
    if let Some(cleanup_sql) = cleanup {
        new_client.send_simple(Query::new(cleanup_sql)).await;
        new_client.read_until('Z').await.unwrap();
    }
}

/// Helper to run a statement and verify schema_changed is NOT set and pools unchanged.
async fn assert_stmt_does_not_set_schema_changed(test_client: &mut TestClient, stmt: &str) {
    // Capture pool IDs before statement
    let pool_ids_before = get_pool_ids(&mut test_client.engine);

    // Verify schema_changed starts as false
    assert!(
        !test_client.engine.router().schema_changed(),
        "schema_changed should be false before statement"
    );

    // Start transaction to observe schema_changed
    test_client.send_simple(Query::new("BEGIN")).await;
    test_client.read_until('Z').await.unwrap();

    // Execute statement
    test_client.send_simple(Query::new(stmt)).await;
    test_client.read_until('Z').await.unwrap();

    // schema_changed should still be false
    assert!(
        !test_client.engine.router().schema_changed(),
        "schema_changed should be false after statement"
    );

    // Pool IDs should be unchanged
    let pool_ids_after = get_pool_ids(&mut test_client.engine);
    assert_eq!(
        pool_ids_before, pool_ids_after,
        "pool IDs should NOT change after non-DDL statement"
    );

    // Commit
    test_client.send_simple(Query::new("COMMIT")).await;
    test_client.read_until('Z').await.unwrap();

    // Verify pools still unchanged after commit
    let pool_ids_final = get_pool_ids(&mut test_client.engine);
    assert_eq!(
        pool_ids_before, pool_ids_final,
        "pool IDs should NOT change after COMMIT of non-DDL transaction"
    );
}

#[tokio::test]
async fn test_create_table_sets_schema_changed() {
    let mut setup_client = TestClient::new_sharded(Parameters::default()).await;
    setup_client
        .send_simple(Query::new("DROP TABLE IF EXISTS test_sc_create"))
        .await;
    setup_client.read_until('Z').await.unwrap();
    drop(setup_client);

    assert_ddl_sets_schema_changed(
        "CREATE TABLE test_sc_create (id INT)",
        "CREATE TABLE",
        Some("DROP TABLE IF EXISTS test_sc_create"),
    )
    .await;
}

#[tokio::test]
async fn test_alter_table_sets_schema_changed() {
    let mut setup_client = TestClient::new_sharded(Parameters::default()).await;
    setup_client
        .send_simple(Query::new("DROP TABLE IF EXISTS test_sc_alter"))
        .await;
    setup_client.read_until('Z').await.unwrap();
    setup_client
        .send_simple(Query::new("CREATE TABLE test_sc_alter (id INT)"))
        .await;
    setup_client.read_until('Z').await.unwrap();
    drop(setup_client);

    assert_ddl_sets_schema_changed(
        "ALTER TABLE test_sc_alter ADD COLUMN name TEXT",
        "ALTER TABLE",
        Some("DROP TABLE IF EXISTS test_sc_alter"),
    )
    .await;
}

#[tokio::test]
async fn test_drop_table_sets_schema_changed() {
    let mut setup_client = TestClient::new_sharded(Parameters::default()).await;
    setup_client
        .send_simple(Query::new(
            "CREATE TABLE IF NOT EXISTS test_sc_drop (id INT)",
        ))
        .await;
    setup_client.read_until('Z').await.unwrap();
    drop(setup_client);

    assert_ddl_sets_schema_changed("DROP TABLE test_sc_drop", "DROP TABLE", None).await;
}

#[tokio::test]
async fn test_create_view_sets_schema_changed() {
    let mut setup_client = TestClient::new_sharded(Parameters::default()).await;
    setup_client
        .send_simple(Query::new("DROP VIEW IF EXISTS test_sc_view"))
        .await;
    setup_client.read_until('Z').await.unwrap();
    drop(setup_client);

    assert_ddl_sets_schema_changed(
        "CREATE VIEW test_sc_view AS SELECT 1 AS col",
        "CREATE VIEW",
        Some("DROP VIEW IF EXISTS test_sc_view"),
    )
    .await;
}

#[tokio::test]
async fn test_schema_changed_cleared_after_commit() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client
        .send_simple(Query::new("DROP TABLE IF EXISTS test_sc_commit"))
        .await;
    test_client.read_until('Z').await.unwrap();

    let pool_ids_before = get_pool_ids(&mut test_client.engine);

    // Start transaction and create table
    test_client.send_simple(Query::new("BEGIN")).await;
    test_client.read_until('Z').await.unwrap();

    test_client
        .send_simple(Query::new("CREATE TABLE test_sc_commit (id INT)"))
        .await;
    test_client.read_until('Z').await.unwrap();

    assert!(
        test_client.engine.router().schema_changed(),
        "schema_changed should be true during transaction"
    );

    // Commit
    test_client.send_simple(Query::new("COMMIT")).await;
    test_client.read_until('Z').await.unwrap();

    assert!(
        !test_client.engine.router().schema_changed(),
        "schema_changed should be cleared after COMMIT"
    );

    drop(test_client);

    // Verify reload with new client
    let mut new_client = TestClient::new_sharded(Parameters::default()).await;
    let pool_ids_after = get_pool_ids(&mut new_client.engine);

    assert!(
        pool_ids_after
            .iter()
            .all(|id| !pool_ids_before.contains(id)),
        "pools should have reloaded after COMMIT"
    );

    // Cleanup
    new_client
        .send_simple(Query::new("DROP TABLE IF EXISTS test_sc_commit"))
        .await;
    new_client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_schema_changed_cleared_after_rollback() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client
        .send_simple(Query::new("DROP TABLE IF EXISTS test_sc_rollback"))
        .await;
    test_client.read_until('Z').await.unwrap();

    let pool_ids_before = get_pool_ids(&mut test_client.engine);

    // Start transaction and create table
    test_client.send_simple(Query::new("BEGIN")).await;
    test_client.read_until('Z').await.unwrap();

    test_client
        .send_simple(Query::new("CREATE TABLE test_sc_rollback (id INT)"))
        .await;
    test_client.read_until('Z').await.unwrap();

    assert!(
        test_client.engine.router().schema_changed(),
        "schema_changed should be true during transaction"
    );

    // Rollback
    test_client.send_simple(Query::new("ROLLBACK")).await;
    test_client.read_until('Z').await.unwrap();

    assert!(
        !test_client.engine.router().schema_changed(),
        "schema_changed should be cleared after ROLLBACK"
    );

    drop(test_client);

    // Verify reload still happens (schema_changed was true)
    let mut new_client = TestClient::new_sharded(Parameters::default()).await;
    let pool_ids_after = get_pool_ids(&mut new_client.engine);

    assert!(
        pool_ids_after
            .iter()
            .all(|id| !pool_ids_before.contains(id)),
        "pools should have reloaded after ROLLBACK (schema_changed was set)"
    );
}

#[tokio::test]
async fn test_ddl_in_multi_statement_transaction() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client
        .send_simple(Query::new("DROP TABLE IF EXISTS test_sc_multi"))
        .await;
    test_client.read_until('Z').await.unwrap();

    // Capture pool IDs before DDL transaction
    let pool_ids_before = get_pool_ids(&mut test_client.engine);

    // Start transaction
    test_client.send_simple(Query::new("BEGIN")).await;
    test_client.read_until('Z').await.unwrap();

    // SELECT should NOT set schema_changed
    test_client.send_simple(Query::new("SELECT 1")).await;
    test_client.read_until('Z').await.unwrap();
    assert!(
        !test_client.engine.router().schema_changed(),
        "schema_changed should be false after SELECT"
    );

    // CREATE TABLE should set schema_changed
    test_client
        .send_simple(Query::new("CREATE TABLE test_sc_multi (id INT)"))
        .await;
    test_client.read_until('Z').await.unwrap();
    assert!(
        test_client.engine.router().schema_changed(),
        "schema_changed should be true after CREATE TABLE"
    );

    // Another SELECT - schema_changed should remain true
    test_client.send_simple(Query::new("SELECT 2")).await;
    test_client.read_until('Z').await.unwrap();
    assert!(
        test_client.engine.router().schema_changed(),
        "schema_changed should persist across statements in transaction"
    );

    // COMMIT clears flag and triggers reload
    test_client.send_simple(Query::new("COMMIT")).await;
    test_client.read_until('Z').await.unwrap();
    assert!(
        !test_client.engine.router().schema_changed(),
        "schema_changed should be cleared after COMMIT"
    );

    // Drop original client to release resources
    drop(test_client);

    // Create a new client - it should see new pool IDs from the reload
    let mut new_client = TestClient::new_sharded(Parameters::default()).await;
    let pool_ids_after = get_pool_ids(&mut new_client.engine);

    assert!(
        pool_ids_after
            .iter()
            .all(|id| !pool_ids_before.contains(id)),
        "new client should see new pool IDs after reload: before={:?}, after={:?}",
        pool_ids_before,
        pool_ids_after
    );

    // Cleanup
    new_client
        .send_simple(Query::new("DROP TABLE IF EXISTS test_sc_multi"))
        .await;
    new_client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_select_does_not_set_schema_changed() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;
    assert_stmt_does_not_set_schema_changed(&mut test_client, "SELECT 1").await;
}

#[tokio::test]
async fn test_insert_does_not_set_schema_changed() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // Setup
    test_client
        .send_simple(Query::new("DROP TABLE IF EXISTS test_sc_ins"))
        .await;
    test_client.read_until('Z').await.unwrap();
    test_client
        .send_simple(Query::new("CREATE TABLE test_sc_ins (id INT)"))
        .await;
    test_client.read_until('Z').await.unwrap();

    // Need a fresh client after setup DDL (which triggers reload)
    drop(test_client);
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    assert_stmt_does_not_set_schema_changed(&mut test_client, "INSERT INTO test_sc_ins VALUES (1)")
        .await;

    // Cleanup
    test_client
        .send_simple(Query::new("DROP TABLE IF EXISTS test_sc_ins"))
        .await;
    test_client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_create_index_does_not_set_schema_changed() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // Setup
    test_client
        .send_simple(Query::new("DROP TABLE IF EXISTS test_sc_idx"))
        .await;
    test_client.read_until('Z').await.unwrap();
    test_client
        .send_simple(Query::new("CREATE TABLE test_sc_idx (id INT)"))
        .await;
    test_client.read_until('Z').await.unwrap();

    // Need a fresh client after setup DDL (which triggers reload)
    drop(test_client);
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    assert_stmt_does_not_set_schema_changed(
        &mut test_client,
        "CREATE INDEX test_sc_idx_i ON test_sc_idx (id)",
    )
    .await;

    // Cleanup
    test_client
        .send_simple(Query::new("DROP TABLE IF EXISTS test_sc_idx"))
        .await;
    test_client.read_until('Z').await.unwrap();
}
