use std::time::Duration;

use tokio::{io::AsyncReadExt, time::timeout};

use super::prelude::*;

/// Regression test: client sends BEGIN, gets response, then sends Terminate.
/// The backend is idle-in-transaction, so it won't send any messages.
/// The client event loop must exit promptly instead of deadlocking.
#[tokio::test]
async fn test_graceful_disconnect_idle_in_transaction() {
    let mut client = SpawnedClient::new_default(Parameters::default()).await;

    // Start a transaction.
    client.send(Query::new("BEGIN")).await;
    client.read_until('Z').await;

    // Send Terminate while idle-in-transaction.
    client.send(Terminate).await;

    // The client task should exit and close the connection.
    // With the deadlock bug, this times out because the event loop hangs.
    let mut buf = [0u8; 1];
    let result = timeout(Duration::from_secs(2), client.conn.read(&mut buf)).await;
    assert!(
        result.is_ok(),
        "client.run() deadlocked after Terminate during idle-in-transaction"
    );

    // EOF means the connection was closed cleanly.
    assert_eq!(result.unwrap().unwrap(), 0);
}
