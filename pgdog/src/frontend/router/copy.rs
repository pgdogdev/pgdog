//! Copy Send clone.

use pgdog_plugin::CopyFormat_CSV;

use crate::net::messages::CopyData;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Copy {
    csv: bool,
    pub(super) headers: bool,
    pub(super) delimiter: char,
    columns: Vec<String>,
    table_name: String,
}

impl Copy {
    /// Assign columns obtained from CSV header.
    pub(super) fn headers(&mut self, headers: &str) {
        self.columns = headers
            .split(self.delimiter)
            .map(|col| col.to_owned())
            .collect();
    }
}

impl From<pgdog_plugin::Copy> for Copy {
    fn from(value: pgdog_plugin::Copy) -> Self {
        Self {
            csv: value.copy_format == CopyFormat_CSV,
            headers: value.has_headers(),
            delimiter: value.delimiter(),
            table_name: value.table_name().to_string(),
            columns: vec![],
        }
    }
}

/// Sharded CopyData message.
#[derive(Debug, Clone)]
pub struct CopyRow {
    row: CopyData,
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
