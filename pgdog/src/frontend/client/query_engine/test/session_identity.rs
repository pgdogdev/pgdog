//! `SET ROLE` and `SET SESSION AUTHORIZATION` must not leak to the
//! next client that gets the same server connection.

use pgdog_config::PoolerMode;

use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test_with_pooler_mode, set},
    expect_message,
    net::{CommandComplete, DataRow, ErrorResponse, ReadyForQuery, RowDescription},
};

use super::prelude::*;

/// Built-in role every PostgreSQL superuser can switch to.
const ROLE: &str = "pg_monitor";

fn load_single_connection_test_pool() {
    load_single_connection_pool(PoolerMode::Transaction);
}

fn load_single_connection_pool(pooler_mode: PoolerMode) {
    load_test_with_pooler_mode(pooler_mode);

    let mut config = (*config()).clone();
    config.config.general.default_pool_size = 1;
    config.config.general.min_pool_size = 0;
    set(config).unwrap();
    reload_from_existing().unwrap();
}

async fn run(client: &mut TestClient, query: &str) {
    client.send_simple(Query::new(query)).await;
    client.read_until('Z').await.unwrap();
}

/// Backend PID, `session_user` and `current_user`.
async fn identity(client: &mut TestClient) -> (i64, String, String) {
    client
        .send_simple(Query::new(
            "SELECT pg_backend_pid(), session_user, current_user",
        ))
        .await;
    expect_message!(client.read().await, RowDescription);
    let row = expect_message!(client.read().await, DataRow);
    expect_message!(client.read().await, CommandComplete);
    expect_message!(client.read().await, ReadyForQuery);

    (
        row.get_int(0, true).expect("backend pid"),
        row.get_text(1).expect("session_user"),
        row.get_text(2).expect("current_user"),
    )
}

async fn show(client: &mut TestClient, setting: &str) -> String {
    client
        .send_simple(Query::new(format!("SHOW {setting}")))
        .await;
    expect_message!(client.read().await, RowDescription);
    let row = expect_message!(client.read().await, DataRow);
    expect_message!(client.read().await, CommandComplete);
    expect_message!(client.read().await, ReadyForQuery);
    row.get_text(0).expect("setting value")
}

/// Disconnect `source` and check the next client gets the same,
/// clean, server connection.
async fn assert_peer_is_clean(source: TestClient, pid: i64) {
    drop(source.leak_pool());

    let mut peer = TestClient::new(Parameters::default()).await;
    let (peer_pid, session_user, current_user) = identity(&mut peer).await;
    assert_eq!(peer_pid, pid, "server connection should be reused");
    assert_eq!(
        (session_user.as_str(), current_user.as_str()),
        ("pgdog", "pgdog")
    );
}

#[tokio::test]
async fn test_role_reset_after_advisory_lock() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;

    run(&mut source, "SELECT pg_advisory_lock(707519)").await;
    run(&mut source, &format!("SET ROLE {ROLE}")).await;

    let (pid, _, current_user) = identity(&mut source).await;
    assert_eq!(current_user, ROLE);
    Box::pin(assert_peer_is_clean(source, pid)).await;
}

#[tokio::test]
async fn test_role_set_inside_connected_transaction() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;

    run(&mut source, "BEGIN").await;
    let (pid, ..) = identity(&mut source).await;
    assert!(source.backend_connected());
    run(&mut source, &format!("SET ROLE {ROLE}")).await;
    run(&mut source, "COMMIT").await;
    assert!(!source.backend_connected());

    // The client keeps its role on the next transaction.
    let (_, _, current_user) = identity(&mut source).await;
    assert_eq!(current_user, ROLE);
    Box::pin(assert_peer_is_clean(source, pid)).await;
}

#[tokio::test]
async fn test_connected_reset_all_keeps_role() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;

    run(&mut source, &format!("SET ROLE {ROLE}")).await;
    assert!(!source.backend_connected());

    run(&mut source, "BEGIN").await;
    let (pid, ..) = identity(&mut source).await;
    run(&mut source, "RESET ALL").await;
    let (_, _, current_user) = identity(&mut source).await;
    assert_eq!(current_user, ROLE, "RESET ALL doesn't reset the role");
    run(&mut source, "COMMIT").await;

    Box::pin(assert_peer_is_clean(source, pid)).await;
}

#[tokio::test]
async fn test_deferred_reset_all_keeps_role() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;

    run(&mut source, &format!("SET ROLE {ROLE}")).await;
    run(&mut source, "RESET ALL").await;
    assert!(!source.backend_connected());

    let (pid, _, current_user) = identity(&mut source).await;
    assert_eq!(current_user, ROLE, "RESET ALL doesn't reset the role");
    Box::pin(assert_peer_is_clean(source, pid)).await;
}

#[tokio::test]
async fn test_deferred_session_authorization() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;

    run(&mut source, &format!("SET SESSION AUTHORIZATION {ROLE}")).await;
    assert!(!source.backend_connected());

    let (pid, session_user, current_user) = identity(&mut source).await;
    assert_eq!((session_user.as_str(), current_user.as_str()), (ROLE, ROLE));
    Box::pin(assert_peer_is_clean(source, pid)).await;
}

#[tokio::test]
async fn test_session_authorization_clears_role() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;

    run(&mut source, "SET ROLE pg_read_all_stats").await;
    run(&mut source, &format!("SET SESSION AUTHORIZATION {ROLE}")).await;

    let (pid, session_user, current_user) = identity(&mut source).await;
    assert_eq!((session_user.as_str(), current_user.as_str()), (ROLE, ROLE));
    Box::pin(assert_peer_is_clean(source, pid)).await;
}

#[tokio::test]
async fn test_role_rollback() {
    load_single_connection_test_pool();
    let mut client = TestClient::new(Parameters::default()).await;

    run(&mut client, "BEGIN").await;
    run(&mut client, &format!("SET ROLE {ROLE}")).await;
    let (pid, _, current_user) = identity(&mut client).await;
    assert_eq!(current_user, ROLE);
    run(&mut client, "ROLLBACK").await;

    let (_, _, current_user) = identity(&mut client).await;
    assert_eq!(current_user, "pgdog");
    Box::pin(assert_peer_is_clean(client, pid)).await;
}

#[tokio::test]
async fn test_deferred_role_inside_transaction() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;

    run(&mut source, "BEGIN").await;
    run(&mut source, &format!("SET ROLE {ROLE}")).await;
    assert!(!source.backend_connected());

    let (pid, _, current_user) = identity(&mut source).await;
    assert_eq!(current_user, ROLE);
    run(&mut source, "COMMIT").await;

    // Committed, so the role is applied again on the next transaction.
    let (_, _, current_user) = identity(&mut source).await;
    assert_eq!(current_user, ROLE);
    Box::pin(assert_peer_is_clean(source, pid)).await;
}

#[tokio::test]
async fn test_rejected_role_inside_transaction() {
    load_single_connection_test_pool();
    let mut client = TestClient::new(Parameters::default()).await;

    run(&mut client, "BEGIN").await;
    let (pid, ..) = identity(&mut client).await;
    client
        .send_simple(Query::new("SET ROLE pgdog_role_does_not_exist"))
        .await;
    expect_message!(client.read().await, ErrorResponse);
    expect_message!(client.read().await, ReadyForQuery);
    run(&mut client, "ROLLBACK").await;

    let (_, _, current_user) = identity(&mut client).await;
    assert_eq!(current_user, "pgdog");
    Box::pin(assert_peer_is_clean(client, pid)).await;
}

/// Any setting changed on a connected server is recorded, not just the role.
#[tokio::test]
async fn test_setting_changed_inside_connected_transaction() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;

    run(&mut source, "BEGIN").await;
    let (pid, ..) = identity(&mut source).await;
    run(&mut source, "SET work_mem TO '1MB'").await;
    run(&mut source, "COMMIT").await;
    assert_eq!(show(&mut source, "work_mem").await, "1MB");
    drop(source.leak_pool());

    let mut peer = TestClient::new(Parameters::default()).await;
    let (peer_pid, ..) = identity(&mut peer).await;
    assert_eq!(peer_pid, pid, "server connection should be reused");
    assert_eq!(show(&mut peer, "work_mem").await, "4MB");
}

/// A failed SET rolls back the SETs before it in the same query,
/// so the server is reset when it goes back to the pool.
#[tokio::test]
async fn test_failed_set_batch_on_connected_server() {
    load_single_connection_pool(PoolerMode::Session);
    let mut source = TestClient::new(Parameters::default()).await;

    let (pid, ..) = identity(&mut source).await;
    assert!(source.backend_connected());
    source
        .send_simple(Query::new(
            "SET work_mem TO '1MB'; SET ROLE pgdog_role_does_not_exist",
        ))
        .await;
    expect_message!(source.read().await, CommandComplete);
    expect_message!(source.read().await, ErrorResponse);
    expect_message!(source.read().await, ReadyForQuery);
    drop(source.leak_pool());

    let mut peer = TestClient::new(Parameters::default()).await;
    let (peer_pid, session_user, current_user) = identity(&mut peer).await;
    assert_eq!(peer_pid, pid, "server connection should be reused");
    assert_eq!(
        (session_user.as_str(), current_user.as_str()),
        ("pgdog", "pgdog")
    );
    assert_eq!(show(&mut peer, "work_mem").await, "4MB");
}
