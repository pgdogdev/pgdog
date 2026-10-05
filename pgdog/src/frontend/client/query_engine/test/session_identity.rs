use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test, set},
    expect_message,
    net::{CommandComplete, DataRow, ErrorResponse, ReadyForQuery, RowDescription},
};

use super::prelude::*;

fn load_single_connection_test_pool() {
    load_test();

    let mut config = (*config()).clone();
    config.config.general.default_pool_size = 1;
    config.config.general.min_pool_size = 0;
    set(config).unwrap();
    reload_from_existing().unwrap();
}

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

async fn create_role(client: &mut TestClient, suffix: &str) -> String {
    let role = format!("pgdog_cleanup_role_{}_{suffix}", std::process::id());
    client
        .send_simple(Query::new(format!("CREATE ROLE {role}")))
        .await;
    client.read_until('Z').await.unwrap();
    role
}

async fn drop_role(client: &mut TestClient, role: &str) {
    client
        .send_simple(Query::new(format!("DROP ROLE {role}")))
        .await;
    client.read_until('Z').await.unwrap();
}

fn assert_default_identity(identity: &(i64, String, String)) {
    assert_eq!(
        (identity.1.as_str(), identity.2.as_str()),
        ("pgdog", "pgdog")
    );
}

#[tokio::test]
async fn test_session_lock_cleanup_resets_role_before_backend_reuse() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;
    let role = create_role(&mut source, "advisory").await;

    source
        .send_simple(Query::new("SELECT pg_advisory_lock(707519)"))
        .await;
    source.read_until('Z').await.unwrap();
    source
        .send_simple(Query::new(format!("SET ROLE {role}")))
        .await;
    source.read_until('Z').await.unwrap();

    let before = identity(&mut source).await;
    assert_eq!(
        (before.1.as_str(), before.2.as_str()),
        ("pgdog", role.as_str())
    );
    drop(source.leak_pool());

    let mut peer = TestClient::new(Parameters::default()).await;
    let after = identity(&mut peer).await;
    assert_eq!(after.0, before.0, "physical backend should be reused");
    assert_default_identity(&after);
    drop_role(&mut peer, &role).await;
}

#[tokio::test]
async fn test_deferred_role_reconciled_before_backend_reuse() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;
    let role = create_role(&mut source, "deferred").await;

    source
        .send_simple(Query::new(format!("SET ROLE {role}")))
        .await;
    source.read_until('Z').await.unwrap();
    assert!(
        !source.backend_connected(),
        "SET ROLE should be deferred until a query checks out a backend"
    );

    let before = identity(&mut source).await;
    assert_eq!(
        (before.1.as_str(), before.2.as_str()),
        ("pgdog", role.as_str())
    );
    assert!(!source.backend_connected());

    source.send_simple(Query::new("RESET ALL")).await;
    source.read_until('Z').await.unwrap();
    assert_eq!(
        source.client().params.session_identity(false).role(),
        Some(role.as_str()),
        "RESET ALL must preserve the frontend's logical role"
    );
    let after_reset_all = identity(&mut source).await;
    assert_eq!(
        after_reset_all, before,
        "PostgreSQL RESET ALL must preserve SET ROLE"
    );
    assert!(!source.backend_connected());
    drop(source.leak_pool());

    let mut peer = TestClient::new(Parameters::default()).await;
    let after = identity(&mut peer).await;
    assert_eq!(after.0, before.0, "physical backend should be reused");
    assert_default_identity(&after);
    drop_role(&mut peer, &role).await;
}

#[tokio::test]
async fn test_session_authorization_reapplied_and_reconciled_before_backend_reuse() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;
    let role = create_role(&mut source, "authorization").await;

    source
        .send_simple(Query::new(format!("SET SESSION AUTHORIZATION {role}")))
        .await;
    source.read_until('Z').await.unwrap();
    assert!(!source.backend_connected());

    let before = identity(&mut source).await;
    assert_eq!(
        (before.1.as_str(), before.2.as_str()),
        (role.as_str(), role.as_str())
    );
    assert!(!source.backend_connected());

    let reapplied = identity(&mut source).await;
    assert_eq!(reapplied, before);
    assert!(!source.backend_connected());
    drop(source.leak_pool());

    let mut peer = TestClient::new(Parameters::default()).await;
    let after = identity(&mut peer).await;
    assert_eq!(after.0, before.0, "physical backend should be reused");
    assert_default_identity(&after);
    drop_role(&mut peer, &role).await;
}

#[tokio::test]
async fn test_deferred_transaction_role_reconciled_before_backend_reuse() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;
    let role = create_role(&mut source, "transaction").await;

    source.send_simple(Query::new("BEGIN")).await;
    source.read_until('Z').await.unwrap();
    source
        .send_simple(Query::new(format!("SET ROLE {role}")))
        .await;
    source.read_until('Z').await.unwrap();
    assert!(
        !source.backend_connected(),
        "BEGIN and SET ROLE should remain deferred until the first query"
    );

    let before = identity(&mut source).await;
    assert_eq!(
        (before.1.as_str(), before.2.as_str()),
        ("pgdog", role.as_str())
    );
    assert!(source.backend_connected());

    source.send_simple(Query::new("COMMIT")).await;
    source.read_until('Z').await.unwrap();
    assert!(!source.backend_connected());

    let reapplied = identity(&mut source).await;
    assert_eq!(
        (reapplied.1.as_str(), reapplied.2.as_str()),
        ("pgdog", role.as_str())
    );
    assert!(!source.backend_connected());
    drop(source.leak_pool());

    let mut peer = TestClient::new(Parameters::default()).await;
    let after = identity(&mut peer).await;
    assert_eq!(after.0, before.0, "physical backend should be reused");
    assert_default_identity(&after);
    drop_role(&mut peer, &role).await;
}

#[tokio::test]
async fn test_transaction_role_rollback_restores_identity() {
    load_single_connection_test_pool();
    let mut client = TestClient::new(Parameters::default()).await;
    let role = create_role(&mut client, "rollback").await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    let original = identity(&mut client).await;

    client
        .send_simple(Query::new(format!("SET ROLE {role}")))
        .await;
    client.read_until('Z').await.unwrap();
    let changed = identity(&mut client).await;
    assert_eq!(changed.0, original.0);
    assert_eq!(
        (changed.1.as_str(), changed.2.as_str()),
        ("pgdog", role.as_str())
    );

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();
    let restored = identity(&mut client).await;
    assert_eq!(restored.0, original.0);
    assert_default_identity(&restored);
    drop_role(&mut client, &role).await;
}

#[tokio::test]
async fn test_rejected_connected_role_restores_identity_on_rollback() {
    load_single_connection_test_pool();
    let mut client = TestClient::new(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    let before = identity(&mut client).await;

    client
        .send_simple(Query::new("SET ROLE pgdog_role_does_not_exist"))
        .await;
    expect_message!(client.read().await, ErrorResponse);
    expect_message!(client.read().await, ReadyForQuery);

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();
    let after = identity(&mut client).await;
    assert_eq!(after.0, before.0);
    assert_default_identity(&after);
}

#[tokio::test]
async fn test_connected_transaction_role_reconciled_before_backend_reuse() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;
    let role = create_role(&mut source, "connected").await;

    source.send_simple(Query::new("BEGIN")).await;
    source.read_until('Z').await.unwrap();

    let connected = identity(&mut source).await;
    assert!(source.backend_connected());

    source
        .send_simple(Query::new(format!("SET ROLE {role}")))
        .await;
    source.read_until('Z').await.unwrap();
    let changed = identity(&mut source).await;
    assert_eq!(changed.0, connected.0);
    assert_eq!(
        (changed.1.as_str(), changed.2.as_str()),
        ("pgdog", role.as_str())
    );

    source.send_simple(Query::new("COMMIT")).await;
    source.read_until('Z').await.unwrap();
    assert!(!source.backend_connected());
    drop(source.leak_pool());

    let mut peer = TestClient::new(Parameters::default()).await;
    let after = identity(&mut peer).await;
    assert_eq!(after.0, connected.0, "physical backend should be reused");
    assert_default_identity(&after);
    drop_role(&mut peer, &role).await;
}
