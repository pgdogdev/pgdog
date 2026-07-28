//! Verify WAL recovery cleans up transactions left by abruptly stopped clients.

use std::{sync::Arc, time::Duration};

use rand::Rng;
use tokio::time::sleep;

use super::*;

#[tokio::test]
async fn test_recovery_cleans_up_aborted_clients() {
    const CLIENTS: usize = 100;

    let client = Arc::new(TwoPcTestClient::new().await);
    assert_eq!(client.prepared_transactions().await, 0);

    let mut handles = Vec::with_capacity(CLIENTS);
    for _ in 0..CLIENTS {
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            loop {
                client.execute().await;
            }
        }));
    }

    // Stagger cancellation so clients are interrupted throughout both 2PC phases.
    for handle in handles {
        let delay = rand::rng().random_range(0..=10);
        sleep(Duration::from_millis(delay)).await;
        handle.abort();
        let _ = handle.await;
    }

    let dangling = client.prepared_transactions().await;
    assert!(
        dangling > 0,
        "expected aborted clients to leave prepared transactions"
    );

    let recovered = client.recover().await;
    recovered.shutdown().await;

    assert_eq!(client.prepared_transactions().await, 0);
    client.shutdown().await;
}
