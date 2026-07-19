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
