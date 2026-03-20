use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::time::timeout;

use crate::net::{ErrorResponse, Parameters, Query};

use super::prelude::*;

#[tokio::test]
async fn test_fatal_error_returns_fatal_error_response() {
    let mut client = SpawnedClient::new_sharded(Parameters::default()).await;

    // Terminate our own backend — PostgreSQL sends a FATAL error.
    client
        .send(Query::new("SELECT pg_terminate_backend(pg_backend_pid())"))
        .await;

    // Read messages off the wire until we get the ErrorResponse.
    let messages = client.read_until('E').await;
    let error_response = ErrorResponse::try_from(messages.last().unwrap().clone()).unwrap();

    assert_eq!(error_response.severity, "FATAL");
    assert_eq!(
        error_response.message,
        "terminating connection due to administrator command"
    );
    assert_eq!(error_response.code, "57P01");

    // No more messages — the socket should be closed (EOF).
    let mut buf = [0u8; 1];
    let n = timeout(Duration::from_secs(1), client.conn.read(&mut buf))
        .await
        .expect("socket read timed out — stream was not closed after FATAL error")
        .expect("read EOF");
    assert_eq!(n, 0, "expected socket to be closed after FATAL error");
}
