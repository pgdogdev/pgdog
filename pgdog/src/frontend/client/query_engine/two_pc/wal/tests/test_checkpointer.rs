use std::{sync::Arc, time::Duration};

use tokio::time::{sleep, timeout};

use super::*;
use crate::frontend::client::query_engine::two_pc::wal::SegmentRegistry;

#[tokio::test]
async fn test_checkpointer_recycles_wal_segments() {
    checkpointer_recycles_wal_segments(50).await;
}

async fn checkpointer_recycles_wal_segments(wal_segment_size: usize) {
    const CLIENTS: usize = 50;
    const TRANSACTIONS_PER_CLIENT: usize = 5;
    const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(2);

    let client = Arc::new(TwoPcTestClient::new(wal_segment_size, Some(CHECKPOINT_INTERVAL)).await);
    let mut handles = Vec::with_capacity(CLIENTS);

    for _ in 0..CLIENTS {
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..TRANSACTIONS_PER_CLIENT {
                client.execute().await;
            }
        }));
    }

    for handle in handles {
        handle.await.expect("checkpointer test client");
    }

    let generated = timeout(Duration::from_secs(1), async {
        loop {
            let segments = SegmentRegistry::get().len();
            if segments > 1 {
                break segments;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("clients should generate multiple WAL segments");

    println!("generated {generated} WAL segments");

    timeout(Duration::from_secs(5), async {
        loop {
            if SegmentRegistry::get().len() == 1 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("checkpointer should recycle inactive WAL segments");

    client.shutdown().await;
}
