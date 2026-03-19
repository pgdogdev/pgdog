use crate::net::{ErrorResponse, Parameters, Query};

use super::prelude::*;

#[tokio::test]
async fn test_transaction_timeout_fatal_forwarded_to_client() {
    let mut client = TestClient::new_basic(Parameters::default()).await;

    client
        .send_simple(Query::new("SET transaction_timeout = '100ms'"))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("SELECT pg_sleep(1)"))
        .await;

    let message = client.read().await;
    assert_eq!(
        message.code(),
        'T',
        "expected RowDescription before FATAL, got '{}'",
        message.code()
    );

    let message = client.read().await;
    assert_eq!(
        message.code(),
        'E',
        "expected ErrorResponse, got '{}'",
        message.code()
    );

    let error = ErrorResponse::try_from(message).unwrap();
    assert_eq!(
        error.severity, "FATAL",
        "expected FATAL severity, got '{}': {}",
        error.severity, error.message
    );
    assert!(
        error.message.contains("transaction timeout"),
        "expected transaction timeout message, got: {}",
        error.message
    );
    assert!(
        !error.message.contains("connection closed by peer"),
        "pgdog should not mask FATAL with its own error"
    );

    assert!(
        !client.backend_connected(),
        "backend should be disconnected after FATAL"
    );
}
