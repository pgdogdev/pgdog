//! Globally unique 64-bit (i64) ID generator.
//!
//! Relies on 2 invariants:
//!
//! 1. Each instance of PgDog must have a unique, numeric NODE_ID,
//!    not exceeding 1023.
//! 2. Each instance of PgDog has reasonably accurate and synchronized
//!    clock, so `std::time::SystemTime` returns a good value.
//!
use std::thread;
use std::time::UNIX_EPOCH;
use std::time::{Duration, SystemTime};

use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use pgdog_config::UniqueIdFunction;
use thiserror::Error;

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

// Compact layout: 41 timestamp + 6 node + 6 sequence = 53 bits (JS-safe)
const COMPACT_NODE_BITS: u64 = 6; // Max 63 nodes
const COMPACT_SEQUENCE_BITS: u64 = 6;
const COMPACT_MAX_NODE_ID: u64 = (1 << COMPACT_NODE_BITS) - 1; // 63
const COMPACT_MAX_SEQUENCE: u64 = (1 << COMPACT_SEQUENCE_BITS) - 1; // 63
const COMPACT_NODE_SHIFT: u8 = COMPACT_SEQUENCE_BITS as u8; // 6
const COMPACT_TIMESTAMP_SHIFT: u8 = (COMPACT_SEQUENCE_BITS + COMPACT_NODE_BITS) as u8; // 12

static UNIQUE_ID: OnceCell<UniqueId> = OnceCell::new();

#[derive(Debug, Default)]
struct State {
    last_timestamp_ms: u64,
    sequence: u64,
}

#[derive(Debug, Default)]
struct CompactState {
    last_timestamp_ms: u64,
    sequence: u64,
}

impl State {
    // Generate next unique ID in a distributed sequence.
    // The `node_id` argument must be globally unique.
    fn next_id(&mut self, node_id: u64, id_offset: u64) -> u64 {
        self.next_id_with(node_id, id_offset, &mut now_ms)
    }

    fn next_id_with<F>(&mut self, node_id: u64, id_offset: u64, now_ms: &mut F) -> u64
    where
        F: FnMut() -> u64,
    {
        let mut now = wait_until_with(self.last_timestamp_ms, now_ms);

        if now == self.last_timestamp_ms {
            self.sequence = (self.sequence + 1) & MAX_SEQUENCE;
            // Wraparound.
            if self.sequence == 0 {
                now = wait_until_with(now + 1, now_ms);
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

impl CompactState {
    fn next_id(&mut self, node_id: u64, id_offset: u64) -> u64 {
        let mut now = wait_until(self.last_timestamp_ms);

        if now == self.last_timestamp_ms {
            self.sequence = (self.sequence + 1) & COMPACT_MAX_SEQUENCE;
            if self.sequence == 0 {
                now = wait_until(now + 1);
            }
        } else {
            self.sequence = 0;
        }

        self.last_timestamp_ms = now;

        let elapsed = self.last_timestamp_ms - PGDOG_EPOCH;
        assert!(
            elapsed <= MAX_TIMESTAMP,
            "unique_id_compact timestamp overflow: {elapsed} > {MAX_TIMESTAMP}"
        );
        let timestamp_part = (elapsed & MAX_TIMESTAMP) << COMPACT_TIMESTAMP_SHIFT;
        let node_part = node_id << COMPACT_NODE_SHIFT;
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

fn wait_until_with<F>(target_ms: u64, now_ms: &mut F) -> u64
where
    F: FnMut() -> u64,
{
    loop {
        let now = now_ms();
        if now >= target_ms {
            return now;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid node identifier: {0}")]
    InvalidNodeId(String),

    #[error("node ID exceeding maximum (1023): {0}")]
    NodeIdTooLarge(u64),

    #[error("node ID exceeding compact maximum (63): {0}")]
    CompactNodeIdTooLarge(u64),

    #[error("id_offset too large, would overflow i64: {0}")]
    OffsetTooLarge(u64),
}

#[derive(Debug)]
pub struct UniqueId {
    node_id: u64,
    id_offset: u64,
    standard: Mutex<State>,
    compact: Mutex<CompactState>,
    function: UniqueIdFunction,
}

impl UniqueId {
    /// Initialize the UniqueId generator.
    fn new(function: UniqueIdFunction) -> Result<Self, Error> {
        let node_id = node_id().map_err(|_| Error::InvalidNodeId(instance_id().to_string()))?;

        let min_id = config().config.general.unique_id_min;

        if node_id > MAX_NODE_ID {
            return Err(Error::NodeIdTooLarge(node_id));
        }

        // Compact (JS-safe) IDs only have 6 node bits.
        if node_id > COMPACT_MAX_NODE_ID {
            return Err(Error::CompactNodeIdTooLarge(node_id));
        }

        if min_id > MAX_OFFSET {
            return Err(Error::OffsetTooLarge(min_id));
        }

        Ok(Self {
            node_id,
            id_offset: min_id,
            standard: Mutex::new(State::default()),
            compact: Mutex::new(CompactState::default()),
            function,
        })
    }

    /// Get (and initialize, if necessary) the unique ID generator.
    pub fn generator() -> Result<&'static UniqueId, Error> {
        UNIQUE_ID.get_or_try_init(|| {
            let config = config();
            Self::new(config.config.general.unique_id_function)
        })
    }

    /// Generate a globally unique, monotonically increasing identifier.
    pub fn next_id(&self) -> i64 {
        match self.function {
            UniqueIdFunction::Compact => {
                self.compact.lock().next_id(self.node_id, self.id_offset) as i64
            }
            UniqueIdFunction::Standard => {
                self.standard.lock().next_id(self.node_id, self.id_offset) as i64
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, env::set_var};

    use super::*;

    fn decode_id(id: u64) -> (u64, u64, u64) {
        let sequence = id & MAX_SEQUENCE;
        let node_id = (id >> NODE_SHIFT) & MAX_NODE_ID;
        let elapsed_ms = id >> TIMESTAMP_SHIFT;
        (elapsed_ms, node_id, sequence)
    }

    fn fake_clock(readings: &[u64]) -> impl FnMut() -> u64 + '_ {
        assert!(
            !readings.is_empty(),
            "fake clock requires at least one reading"
        );

        let mut index = 0;
        let last = readings[readings.len() - 1];

        move || {
            let reading = readings.get(index).copied().unwrap_or(last);
            index += 1;
            reading
        }
    }

    #[test]
    fn test_unique_ids() {
        unsafe {
            set_var("NODE_ID", "pgdog-1");
        }
        let num_ids = 10_000;

        let mut ids = HashSet::new();

        for _ in 0..num_ids {
            ids.insert(UniqueId::generator().unwrap().next_id());
        }

        assert_eq!(ids.len(), num_ids);
    }

    #[test]
    fn test_ids_monotonically_increasing() {
        let mut state = State::default();
        let node_id = 1u64;

        let mut prev_id = 0u64;
        for _ in 0..10_000 {
            let id = state.next_id(node_id, 0);
            assert!(id > prev_id, "ID {id} not greater than previous {prev_id}");
            prev_id = id;
        }
    }

    #[test]
    fn test_ids_always_positive() {
        let mut state = State::default();
        let node_id = MAX_NODE_ID; // Use max node to maximize bits used

        for _ in 0..10_000 {
            let id = state.next_id(node_id, 0);
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

    #[test]
    fn test_extract_components() {
        let node: u64 = 42;
        let mut state = State::default();

        let id = state.next_id(node, 0);

        // Extract components back
        let extracted_seq = id & MAX_SEQUENCE;
        let extracted_node = (id >> NODE_SHIFT) & MAX_NODE_ID;
        let extracted_elapsed = id >> TIMESTAMP_SHIFT;

        assert_eq!(extracted_node, node);
        assert_eq!(extracted_seq, 0); // First ID has sequence 0
        assert!(extracted_elapsed > 0); // Elapsed time since epoch

        // Generate another ID and verify sequence increments
        let id2 = state.next_id(node, 0);
        let extracted_seq2 = id2 & MAX_SEQUENCE;
        let extracted_node2 = (id2 >> NODE_SHIFT) & MAX_NODE_ID;

        assert_eq!(extracted_node2, node);
        assert!(matches!(extracted_seq2, 1 | 0)); // Sequence incremented (or time advanced and reset to 0)
    }

    #[test]
    fn test_regression_fixed_clock_ids() {
        let node = 42;
        let now = 1_775_590_268_126;
        let mut state = State::default();
        let readings = [now, now, now + 1];
        let mut now_ms = fake_clock(&readings);

        let first = state.next_id_with(node, 0, &mut now_ms);
        let second = state.next_id_with(node, 0, &mut now_ms);
        let third = state.next_id_with(node, 0, &mut now_ms);

        assert_eq!(first, 47_839_699_276_046_336);
        assert_eq!(second, 47_839_699_276_046_337);
        assert_eq!(third, 47_839_699_280_240_640);

        assert_eq!(decode_id(first), (11_405_873_126, node, 0));
        assert_eq!(decode_id(second), (11_405_873_126, node, 1));
        assert_eq!(decode_id(third), (11_405_873_127, node, 0));
    }

    #[test]
    fn test_regression_sequence_wrap_waits_for_next_millisecond() {
        let node = 7;
        let elapsed_ms = 99;
        let now = PGDOG_EPOCH + elapsed_ms;
        let mut state = State {
            last_timestamp_ms: now,
            sequence: MAX_SEQUENCE - 1,
        };
        let readings = [now, now, now, now + 1];
        let mut now_ms = fake_clock(&readings);

        let last_in_millisecond = state.next_id_with(node, 0, &mut now_ms);
        let first_in_next_millisecond = state.next_id_with(node, 0, &mut now_ms);

        assert_eq!(last_in_millisecond, 415_268_863);
        assert_eq!(first_in_next_millisecond, 419_459_072);
        assert!(
            first_in_next_millisecond > last_in_millisecond,
            "IDs must remain monotonic across sequence wrap"
        );
    }

    #[test]
    fn test_regression_offset_is_applied_after_packed_id() {
        let node = 3;
        let elapsed_ms = 321;
        let offset = 1_000_000;
        let now = PGDOG_EPOCH + elapsed_ms;
        let mut state = State::default();
        let readings = [now];
        let mut now_ms = fake_clock(&readings);

        let id = state.next_id_with(node, offset, &mut now_ms);

        assert_eq!(id, 1_347_383_872);
    }

    #[test]
    fn test_id_offset() {
        let offset: u64 = 1_000_000_000;
        let node: u64 = 5;
        let mut state = State::default();

        for _ in 0..1000 {
            let id = state.next_id(node, offset);
            assert!(
                id > offset,
                "ID {id} should be greater than offset {offset}"
            );
        }
    }

    #[test]
    fn test_compact_unique_ids() {
        let num_ids = 10_000;
        let mut ids = HashSet::new();
        let mut state = CompactState::default();
        let node_id = 1u64;

        for _ in 0..num_ids {
            ids.insert(state.next_id(node_id, 0));
        }

        assert_eq!(ids.len(), num_ids);
    }

    #[test]
    fn test_compact_monotonically_increasing() {
        let mut state = CompactState::default();
        let node_id = 1u64;

        let mut prev_id = 0u64;
        for _ in 0..10_000 {
            let id = state.next_id(node_id, 0);
            assert!(id > prev_id, "ID {id} not greater than previous {prev_id}");
            prev_id = id;
        }
    }

    #[test]
    fn test_compact_js_safe() {
        let mut state = CompactState::default();
        let node_id = COMPACT_MAX_NODE_ID;
        const JS_MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

        for _ in 0..10_000 {
            let id = state.next_id(node_id, 0);
            let signed = id as i64;
            assert!(signed > 0, "ID should be positive, got {signed}");
            assert!(
                id <= JS_MAX_SAFE_INTEGER,
                "ID {id} exceeds JS MAX_SAFE_INTEGER {JS_MAX_SAFE_INTEGER}"
            );
        }
    }

    #[test]
    fn test_compact_bit_layout() {
        // 41 timestamp + 6 node + 6 sequence = 53 bits
        assert_eq!(
            TIMESTAMP_BITS + COMPACT_NODE_BITS + COMPACT_SEQUENCE_BITS,
            53
        );
        assert_eq!(COMPACT_TIMESTAMP_SHIFT, 12);
        assert_eq!(COMPACT_NODE_SHIFT, 6);
    }

    #[test]
    fn test_compact_max_values_fit() {
        let max_elapsed = MAX_TIMESTAMP;
        let max_node = COMPACT_MAX_NODE_ID;
        let max_seq = COMPACT_MAX_SEQUENCE;

        let id =
            (max_elapsed << COMPACT_TIMESTAMP_SHIFT) | (max_node << COMPACT_NODE_SHIFT) | max_seq;
        let signed = id as i64;
        const JS_MAX_SAFE_INTEGER: i64 = (1 << 53) - 1;

        assert!(
            signed > 0,
            "Max compact ID should be positive, got {signed}"
        );
        assert!(
            signed <= JS_MAX_SAFE_INTEGER,
            "Max compact ID {signed} exceeds JS MAX_SAFE_INTEGER {JS_MAX_SAFE_INTEGER}"
        );
    }

    #[test]
    fn test_compact_extract_components() {
        let node: u64 = 42;
        let mut state = CompactState::default();

        let id = state.next_id(node, 0);

        let extracted_seq = id & COMPACT_MAX_SEQUENCE;
        let extracted_node = (id >> COMPACT_NODE_SHIFT) & COMPACT_MAX_NODE_ID;
        let extracted_elapsed = id >> COMPACT_TIMESTAMP_SHIFT;

        assert_eq!(extracted_node, node);
        assert_eq!(extracted_seq, 0);
        assert!(extracted_elapsed > 0);

        let id2 = state.next_id(node, 0);
        let extracted_seq2 = id2 & COMPACT_MAX_SEQUENCE;
        let extracted_node2 = (id2 >> COMPACT_NODE_SHIFT) & COMPACT_MAX_NODE_ID;

        assert_eq!(extracted_node2, node);
        assert!(matches!(extracted_seq2, 1 | 0));
    }

    #[test]
    fn test_id_offset_monotonic() {
        let offset: u64 = 1_000_000_000;
        let node: u64 = 5;
        let mut state = State::default();

        let mut prev_id = 0u64;
        for _ in 0..1000 {
            let id = state.next_id(node, offset);
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
