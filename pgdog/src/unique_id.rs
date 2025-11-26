use std::sync::Arc;
use std::time::UNIX_EPOCH;
use std::time::{Duration, SystemTime};

use once_cell::sync::OnceCell;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::util::{instance_id, node_id};

const NODE_BITS: u64 = 10; // Max 1023 nodes
const SEQUENCE_BITS: u64 = 12;
const MAX_NODE_ID: u64 = (1 << NODE_BITS) - 1; // 1023
const MAX_SEQUENCE: u64 = (1 << SEQUENCE_BITS) - 1; // 4095
const PGDOG_EPOCH: u64 = 1764184395000; // Wednesday, November 26, 2025 11:13:15 AM GMT-08:00
const NODE_SHIFT: u8 = SEQUENCE_BITS as u8; // 12
const TIMESTAMP_SHIFT: u8 = (SEQUENCE_BITS + NODE_BITS) as u8; // 22

static UNIQUE_ID: OnceCell<UniqueId> = OnceCell::new();

#[derive(Debug, Default)]
struct State {
    last_timestamp_ms: u64,
    sequence: u64,
}

impl State {
    // Generate next unique ID in a distributed sequence.
    // The `node_id` argument must be globally unique.
    async fn next_id(&mut self, node_id: u64) -> u64 {
        let mut now = wait_until(self.last_timestamp_ms).await;

        if now == self.last_timestamp_ms {
            self.sequence = (self.sequence + 1) & MAX_SEQUENCE;
            // Wraparound.
            if self.sequence == 0 {
                now = wait_until(now + 1).await;
            }
        } else {
            // Reset sequence to zero once we reach next ms.
            self.sequence = 0;
        }

        self.last_timestamp_ms = now;

        let timestamp_part = (self.last_timestamp_ms - PGDOG_EPOCH) << TIMESTAMP_SHIFT;
        let node_part = node_id << NODE_SHIFT;
        let sequence_part = self.sequence;

        timestamp_part | node_part | sequence_part
    }
}

// Get current time in ms.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("SystemTime is before UNIX_EPOCH")
        .as_millis() as u64
}

// Get a monotonically increasing timestamp in ms.
// Protects against clock drift.
async fn wait_until(target_ms: u64) -> u64 {
    loop {
        let now = now_ms();
        if now >= target_ms {
            return now;
        }
        sleep(Duration::from_millis(1)).await;
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid node identifier: {0}")]
    InvalidNodeId(String),

    #[error("node ID exceeding maximum (1023): {0}")]
    NodeIdTooLarge(u64),
}

#[derive(Debug, Clone)]
pub struct UniqueId {
    node_id: u64,
    inner: Arc<Mutex<State>>,
}

impl UniqueId {
    /// Initiliaze the UniqueId generator.
    fn new() -> Result<Self, Error> {
        let node_id = node_id().map_err(|_| Error::InvalidNodeId(instance_id().to_string()))?;

        if node_id > MAX_NODE_ID {
            return Err(Error::NodeIdTooLarge(node_id));
        }

        Ok(Self {
            node_id,
            inner: Arc::new(Mutex::new(State::default())),
        })
    }

    /// Get (and initialize, if necessary) the unique ID generator.
    pub fn generator() -> Result<&'static UniqueId, Error> {
        UNIQUE_ID.get_or_try_init(|| Self::new())
    }

    /// Generate a globally unique, monotonically increasing identifier.
    pub async fn next_id(&self) -> i64 {
        self.inner.lock().await.next_id(self.node_id).await as i64
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, env::set_var};

    use super::*;

    #[tokio::test]
    async fn test_unique_ids() {
        unsafe {
            set_var("NODE_ID", "pgdog-1");
        }
        let num_ids = 10_000;

        let mut ids = HashSet::new();

        for _ in 0..num_ids {
            ids.insert(UniqueId::generator().unwrap().next_id().await);
        }

        println!("{:?}", ids);

        assert_eq!(ids.len(), num_ids);
    }
}
