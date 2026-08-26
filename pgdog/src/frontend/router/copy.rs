//! Copy Send clone.

use crate::net::messages::CopyData;

use super::parser::Shard;

/// Sharded CopyData message.
#[derive(Debug, Clone)]
pub(crate) struct CopyRow {
    row: CopyData,
    /// If shard is none, row should go to all shards.
    shard: Shard,
}

impl CopyRow {
    /// Create new copy row for given shard.
    pub(crate) fn new(data: &[u8], shard: Shard) -> Self {
        Self {
            row: CopyData::new(data),
            shard,
        }
    }

    /// Send copy row to all shards.
    pub(crate) fn omnishard(row: CopyData) -> Self {
        Self {
            row,
            shard: Shard::All,
        }
    }

    /// Which shard it should go to.
    pub(crate) fn shard(&self) -> &Shard {
        &self.shard
    }

    /// Get message data.
    pub(crate) fn message(&self) -> CopyData {
        self.row.clone()
    }

    /// Length of the message.
    pub(crate) fn len(&self) -> usize {
        self.row.len()
    }
}
