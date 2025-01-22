//! Copy Send clone.

use pgdog_plugin::CopyFormat_CSV;

use crate::net::messages::CopyData;

/// Sharded copy initial state.
///
/// Indicates that rows will be split between shards by a plugin.
///
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ShardedCopy {
    csv: bool,
    pub(super) headers: bool,
    pub(super) delimiter: char,
    pub(super) sharded_column: usize,
}

impl ShardedCopy {
    /// Create new sharded copy state.
    pub fn new(copy: pgdog_plugin::Copy, sharded_column: usize) -> Self {
        Self {
            csv: copy.copy_format == CopyFormat_CSV,
            headers: copy.has_headers(),
            delimiter: copy.delimiter(),
            sharded_column,
        }
    }
}

/// Sharded CopyData message.
#[derive(Debug, Clone)]
pub struct CopyRow {
    row: CopyData,
    /// If shard is none, row should go to all shards.
    shard: Option<usize>,
}

impl CopyRow {
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
