use crate::{
    expect_message,
    net::{ErrorResponse, Parameters, ReadyForQuery},
};

use super::prelude::*;

const MIXED_SET_ERROR: &str = "multi-statement queries cannot mix SET with other commands";

fn assert_mixed_set_error(error: ErrorResponse) {
    assert!(
        error.message.contains(MIXED_SET_ERROR),
        "unexpected error: {error:?}",
    );
}

async fn assert_connection_usable(client: &mut TestClient) {
    client.send_simple(Query::new("SELECT 1")).await;
    let messages = client.read_until('Z').await.unwrap();
    assert!(
        messages.iter().any(|message| message.code() == 'C'),
        "expected a successful query response: {messages:?}",
    );
}

#[tokio::test]
async fn mixed_set_simple_returns_error() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send(Query::new("SET statement_timeout TO '10s'; SELECT 1"))
        .await;
    client.try_process().await.unwrap();

    assert_mixed_set_error(expect_message!(client.read().await, ErrorResponse));
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'I'
    );
    assert!(!client.backend_connected());

    assert_connection_usable(&mut client).await;
}

#[tokio::test]
async fn mixed_set_extended_returns_error() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send(Parse::named(
            "mixed",
            "SET statement_timeout TO '10s'; SELECT 1",
        ))
        .await;
    client.send(Bind::new_statement("mixed")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    assert_mixed_set_error(expect_message!(client.read().await, ErrorResponse));
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'I'
    );
    assert!(!client.backend_connected());

    assert_connection_usable(&mut client).await;
}
