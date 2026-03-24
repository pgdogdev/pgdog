use crate::{
    config::Role,
    expect_message,
    net::{ErrorResponse, Parameters, Protocol, Query, ReadyForQuery},
};

use super::test_client::TestClient;

#[tokio::test]
async fn test_target_session_attrs_standby() {
    let mut params = Parameters::default();
    params.insert("pgdog.role", "replica");

    let mut client = TestClient::new_replicas(params).await;
    assert_eq!(client.client().sticky.role, Some(Role::Replica));

    client
        .send_simple(Query::new(
            "CREATE TABLE test_target_session_attrs_standby(id BIGINT)",
        ))
        .await;

    let err = expect_message!(client.read().await, ErrorResponse);
    assert_eq!(
        err.message,
        "cannot execute CREATE TABLE in a read-only transaction"
    );
    expect_message!(client.read().await, ReadyForQuery);
}

#[tokio::test]
async fn test_target_session_attrs_primary() {
    let mut params = Parameters::default();
    params.insert("pgdog.role", "primary");

    let mut client = TestClient::new_replicas(params).await;
    assert_eq!(client.client().sticky.role, Some(Role::Primary));

    for _ in 0..5 {
        client
            .send_simple(Query::new(
                "CREATE TABLE IF NOT EXISTS test_target_session_attrs_primary(id BIGINT)",
            ))
            .await;

        // Read until ReadyForQuery — may include NOTICE messages.
        for msg in client.read_until('Z').await.unwrap() {
            assert_ne!(msg.code(), 'E');
        }
    }
}
