//! Copy Send clone.

use crate::net::messages::CopyData;

use super::parser::Shard;

/// Sharded CopyData message.
#[derive(Debug, Clone)]
pub struct CopyRow {
    row: CopyData,
    /// If shard is none, row should go to all shards.
    shard: Shard,
}

impl CopyRow {
    /// Create new copy row for given shard.
    pub fn new(data: &[u8], shard: Shard) -> Self {
        Self {
            row: CopyData::new(data),
            shard,
        }
    }

    /// Send copy row to all shards.
    pub fn omnishard(row: CopyData) -> Self {
        Self {
            row,
            shard: Shard::All,
        }
    }

    /// Which shard it should go to.
    pub fn shard(&self) -> &Shard {
        &self.shard
    }

    /// Get message data.
    pub fn message(&self) -> CopyData {
        self.row.clone()
    }

    /// Create new headers message that should go to all shards.
    pub fn headers(headers: &str) -> Self {
        Self {
            shard: Shard::All,
            row: CopyData::new(headers.as_bytes()),
        }
    }
}
