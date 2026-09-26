use std::time::Duration;

use tokio::time::timeout;

use crate::{
    expect_message,
    frontend::comms::comms,
    net::{ErrorResponse, Parameters},
};

use super::prelude::*;

/// Test that a graceful shutdown sends the "shutting down" error to the client
/// and the client loop exits.
#[tokio::test]
async fn test_graceful_shutdown_sends_error() {
    let mut client = SpawnedClient::new_default(Parameters::default()).await;

    // Trigger global shutdown.
    comms().shutdown();

    let err = expect_message!(client.read().await, ErrorResponse);
    assert_eq!(err.code, "57P01");
    assert_eq!(err.message, "PgDog is shutting down");

    // Client loop should exit promptly.
    timeout(Duration::from_secs(2), client.join())
        .await
        .expect("client loop did not exit after shutdown");
}

/// Test that a client inside a transaction is allowed to finish
/// before receiving the shutdown error.
#[tokio::test]
async fn test_graceful_shutdown_waits_for_transaction() {
    let mut client = SpawnedClient::new_default(Parameters::default()).await;

    // Start a transaction.
    client.send(Query::new("BEGIN")).await;
    client.read_until('Z').await;

    // Trigger shutdown while in transaction.
    comms().shutdown();

    // Client should still be able to complete the transaction.
    client.send(Query::new("SELECT 1")).await;
    client.read_until('Z').await;

    client.send(Query::new("COMMIT")).await;
    client.read_until('Z').await;

    // Now that the transaction is done, client receives the shutdown error.
    let err = expect_message!(client.read().await, ErrorResponse);
    assert_eq!(err.code, "57P01");
    assert_eq!(err.message, "PgDog is shutting down");

    // Client loop should exit promptly.
    timeout(Duration::from_secs(2), client.join())
        .await
        .expect("client loop did not exit after shutdown");
}

/// Test that a request whose bytes already arrived when shutdown starts
/// is served before the shutdown error. `yield_now` lets the I/O driver
/// observe the readable socket, so the client loop sees the request and the
/// shutdown signal ready in the same poll.
#[tokio::test]
async fn test_graceful_shutdown_serves_received_request_first() {
    let mut client = SpawnedClient::new_default(Parameters::default()).await;

    client.send(Query::new("SELECT 1")).await;
    tokio::task::yield_now().await;
    comms().shutdown();

    let codes: Vec<char> = client
        .read_until('Z')
        .await
        .iter()
        .map(|message| message.code())
        .collect();
    assert_eq!(codes, vec!['T', 'D', 'C', 'Z']);

    let err = expect_message!(client.read().await, ErrorResponse);
    assert_eq!(err.code, "57P01");

    timeout(Duration::from_secs(2), client.join())
        .await
        .expect("client loop did not exit after shutdown");
}

/// Test that a partially received extended-protocol request is completed
/// before the client receives the shutdown error.
#[tokio::test]
async fn test_graceful_shutdown_waits_for_partial_extended_request() {
    let mut client = SpawnedClient::new_default(Parameters::default()).await;

    client.send(Parse::named("shutdown_test", "SELECT 1")).await;
    client.send(Bind::new_statement("shutdown_test")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    comms().shutdown();
    tokio::time::sleep(Duration::from_millis(50)).await;

    client.send(Execute::new()).await;
    client.send(Sync::new()).await;

    let codes: Vec<char> = client
        .read_until('Z')
        .await
        .iter()
        .map(|message| message.code())
        .collect();
    assert_eq!(codes, vec!['1', '2', 'D', 'C', 'Z']);

    let err = expect_message!(client.read().await, ErrorResponse);
    assert_eq!(err.code, "57P01");

    timeout(Duration::from_secs(2), client.join())
        .await
        .expect("client loop did not exit after shutdown");
}
