//! Verify WAL recovery cleans up transactions left by abruptly stopped clients.

use std::{sync::Arc, time::Duration};

use rand::Rng;
use tokio::time::sleep;

use super::*;
use crate::frontend::client::query_engine::two_pc::wal::SegmentRegistry;

macro_rules! chaos_test {
    ($name:ident, $wal_segment_size:expr) => {
        #[tokio::test]
        async fn $name() {
            recovery_cleans_up_aborted_clients($wal_segment_size, None).await;
        }
    };
}

chaos_test!(test_recovery_with_50_byte_segments, 50);
chaos_test!(test_recovery_with_100_byte_segments, 100);
chaos_test!(test_recovery_with_1000_byte_segments, 1_000);
chaos_test!(test_recovery_with_1_mb_segments, 1_000_000);
chaos_test!(test_recovery_with_16_mb_segments, 16_000_000);

pub(super) async fn recovery_cleans_up_aborted_clients(
    wal_segment_size: usize,
    checkpoint_interval: Option<Duration>,
) {
    const CLIENTS: usize = 100;

    let client = Arc::new(TwoPcTestClient::new(wal_segment_size, checkpoint_interval).await);
    assert_eq!(client.prepared_transactions().await, 0);
    let transactions_before = client.pool_transactions();

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

    let transactions = client
        .pool_transactions()
        .saturating_sub(transactions_before);
    assert!(
        transactions > 0,
        "expected clients to execute transactions before cancellation"
    );

    let dangling = client.prepared_transactions().await;
    assert!(
        dangling > 0,
        "expected aborted clients to leave prepared transactions"
    );

    let wal_segments_created = SegmentRegistry::get().len();
    assert!(
        wal_segments_created > 0,
        "expected the chaos workload to create WAL segments"
    );

    let recovered = client.recover().await;
    recovered.shutdown().await;

    assert_eq!(client.prepared_transactions().await, 0);
}
