//! Copy Send clone.

use crate::net::messages::CopyData;

/// Sharded CopyData message.
#[derive(Debug, Clone)]
pub struct CopyRow {
    row: CopyData,
    /// If shard is none, row should go to all shards.
    shard: Option<usize>,
}

impl CopyRow {
    /// Create new copy row for given shard.
    pub fn new(data: &[u8], shard: Option<usize>) -> Self {
        Self {
            row: CopyData::new(data),
            shard,
        }
    }

    /// Send copy row to all shards.
    pub fn omnishard(row: CopyData) -> Self {
        Self { row, shard: None }
    }

    /// Which shard it should go to.
    pub fn shard(&self) -> Option<usize> {
        self.shard
    }

    /// Get message data.
    pub fn message(&self) -> CopyData {
        self.row.clone()
    }

    /// Create new headers message that should go to all shards.
    pub fn headers(headers: &str) -> Self {
        Self {
            shard: None,
            row: CopyData::new(headers.as_bytes()),
        }
    }
}

impl From<pgdog_plugin::CopyRow> for CopyRow {
    fn from(value: pgdog_plugin::CopyRow) -> Self {
        let row = CopyData::new(value.data());
        Self {
            row,
            shard: Some(value.shard()),
        }
    }
}
