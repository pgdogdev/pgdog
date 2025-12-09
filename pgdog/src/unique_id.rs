//! Globally unique 64-bit (i64) ID generator.
//!
//! Relies on 2 invariants:
//!
//! 1. Each instance of PgDog must have a unique, numeric NODE_ID,
//!    not exceeding 1023.
//! 2. Each instance of PgDog has reasonably accurate and synchronized
//!    clock, so `std::time::SystemTime` returns a good value.
//!
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use std::time::{Duration, SystemTime};

use once_cell::sync::OnceCell;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::config::config;
use crate::util::{instance_id, node_id};

const NODE_BITS: u64 = 10; // Max 1023 nodes
const SEQUENCE_BITS: u64 = 12;
const TIMESTAMP_BITS: u64 = 41; // 41 bits = ~69 years, keeps i64 sign bit clear
const MAX_NODE_ID: u64 = (1 << NODE_BITS) - 1; // 1023
const MAX_SEQUENCE: u64 = (1 << SEQUENCE_BITS) - 1; // 4095
const MAX_TIMESTAMP: u64 = (1 << TIMESTAMP_BITS) - 1;
const PGDOG_EPOCH: u64 = 1764184395000; // Wednesday, November 26, 2025 11:13:15 AM GMT-08:00
const NODE_SHIFT: u8 = SEQUENCE_BITS as u8; // 12
const TIMESTAMP_SHIFT: u8 = (SEQUENCE_BITS + NODE_BITS) as u8; // 22
                                                               // Maximum offset to ensure base_id + offset doesn't overflow i64
const MAX_OFFSET: u64 = i64::MAX as u64
    - ((MAX_TIMESTAMP << TIMESTAMP_SHIFT) | (MAX_NODE_ID << NODE_SHIFT) | MAX_SEQUENCE);

static UNIQUE_ID: OnceCell<UniqueId> = OnceCell::new();

#[derive(Debug, Default)]
struct State {
    last_timestamp_ms: u64,
    sequence: u64,
}

impl State {
    // Generate next unique ID in a distributed sequence.
    // The `node_id` argument must be globally unique.
    async fn next_id(&mut self, node_id: u64, id_offset: u64) -> u64 {
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

        let elapsed = self.last_timestamp_ms - PGDOG_EPOCH;
        assert!(
            elapsed <= MAX_TIMESTAMP,
            "unique_id timestamp overflow: {elapsed} > {MAX_TIMESTAMP}"
        );
        let timestamp_part = (elapsed & MAX_TIMESTAMP) << TIMESTAMP_SHIFT;
        let node_part = node_id << NODE_SHIFT;
        let sequence_part = self.sequence;

        let base_id = timestamp_part | node_part | sequence_part;
        base_id + id_offset
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

    #[error("id_offset too large, would overflow i64: {0}")]
    OffsetTooLarge(u64),
}

#[derive(Debug, Clone)]
pub struct UniqueId {
    node_id: u64,
    id_offset: u64,
    inner: Arc<Mutex<State>>,
}

impl UniqueId {
    /// Initialize the UniqueId generator.
    fn new() -> Result<Self, Error> {
        let node_id = node_id().map_err(|_| Error::InvalidNodeId(instance_id().to_string()))?;

        let min_id = config().config.general.unique_id_min;

        if node_id > MAX_NODE_ID {
            return Err(Error::NodeIdTooLarge(node_id));
        }

        if min_id > MAX_OFFSET {
            return Err(Error::OffsetTooLarge(min_id));
        }

        Ok(Self {
            node_id,
            id_offset: min_id,
            inner: Arc::new(Mutex::new(State::default())),
        })
    }

    /// Get (and initialize, if necessary) the unique ID generator.
    pub fn generator() -> Result<&'static UniqueId, Error> {
        UNIQUE_ID.get_or_try_init(Self::new)
    }

    /// Generate a globally unique, monotonically increasing identifier.
    pub async fn next_id(&self) -> i64 {
        self.inner
            .lock()
            .await
            .next_id(self.node_id, self.id_offset)
            .await as i64
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

        assert_eq!(ids.len(), num_ids);
    }

    #[tokio::test]
    async fn test_ids_monotonically_increasing() {
        let mut state = State::default();
        let node_id = 1u64;

        let mut prev_id = 0u64;
        for _ in 0..10_000 {
            let id = state.next_id(node_id, 0).await;
            assert!(id > prev_id, "ID {id} not greater than previous {prev_id}");
            prev_id = id;
        }
    }

    #[tokio::test]
    async fn test_ids_always_positive() {
        let mut state = State::default();
        let node_id = MAX_NODE_ID; // Use max node to maximize bits used

        for _ in 0..10_000 {
            let id = state.next_id(node_id, 0).await;
            let signed = id as i64;
            assert!(signed > 0, "ID should be positive, got {signed}");
        }
    }

    #[test]
    fn test_bit_layout() {
        // Verify the bit allocation: 41 timestamp + 10 node + 12 sequence = 63 bits
        assert_eq!(TIMESTAMP_BITS + NODE_BITS + SEQUENCE_BITS, 63);
        assert_eq!(TIMESTAMP_SHIFT, 22);
        assert_eq!(NODE_SHIFT, 12);
    }

    #[test]
    fn test_max_values_fit() {
        // Construct an ID with max values and verify it stays positive
        let max_elapsed = MAX_TIMESTAMP;
        let max_node = MAX_NODE_ID;
        let max_seq = MAX_SEQUENCE;

        let id = (max_elapsed << TIMESTAMP_SHIFT) | (max_node << NODE_SHIFT) | max_seq;
        let signed = id as i64;

        assert!(signed > 0, "Max ID should be positive, got {signed}");
        assert_eq!(id >> 63, 0, "Bit 63 should be clear");
    }

    #[tokio::test]
    async fn test_extract_components() {
        let node: u64 = 42;
        let mut state = State::default();

        let id = state.next_id(node, 0).await;

        // Extract components back
        let extracted_seq = id & MAX_SEQUENCE;
        let extracted_node = (id >> NODE_SHIFT) & MAX_NODE_ID;
        let extracted_elapsed = id >> TIMESTAMP_SHIFT;

        assert_eq!(extracted_node, node);
        assert_eq!(extracted_seq, 0); // First ID has sequence 0
        assert!(extracted_elapsed > 0); // Elapsed time since epoch

        // Generate another ID and verify sequence increments
        let id2 = state.next_id(node, 0).await;
        let extracted_seq2 = id2 & MAX_SEQUENCE;
        let extracted_node2 = (id2 >> NODE_SHIFT) & MAX_NODE_ID;

        assert_eq!(extracted_node2, node);
        assert!(matches!(extracted_seq2, 1 | 0)); // Sequence incremented (or time advanced and reset to 0)
    }

    #[tokio::test]
    async fn test_id_offset() {
        let offset: u64 = 1_000_000_000;
        let node: u64 = 5;
        let mut state = State::default();

        for _ in 0..1000 {
            let id = state.next_id(node, offset).await;
            assert!(
                id > offset,
                "ID {id} should be greater than offset {offset}"
            );
        }
    }

    #[tokio::test]
    async fn test_id_offset_monotonic() {
        let offset: u64 = 1_000_000_000;
        let node: u64 = 5;
        let mut state = State::default();

        let mut prev_id = 0u64;
        for _ in 0..1000 {
            let id = state.next_id(node, offset).await;
            assert!(id > prev_id, "ID {id} not greater than previous {prev_id}");
            prev_id = id;
        }
    }

    #[test]
    fn test_max_offset() {
        // Verify MAX_OFFSET calculation is correct
        let max_base_id =
            (MAX_TIMESTAMP << TIMESTAMP_SHIFT) | (MAX_NODE_ID << NODE_SHIFT) | MAX_SEQUENCE;
        let result = max_base_id + MAX_OFFSET;
        assert!(result <= i64::MAX as u64, "MAX_OFFSET would overflow i64");
    }
}
