//! Copy-data task definition and status, and the per-table copy subtask.

use derive_more::Display;
use pgdog_config::CopyFormat;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::Databases;

/// The bulk data copy one copy-data task runs.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("copy_data {databases}")]
pub struct CopyDataDefinition {
    pub databases: Databases,
    pub format: CopyFormat,
}

/// Stages of a bulk data copy, reported as the task's status. Per-table
/// progress lives on the [`TableCopyStatus`] child tasks.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[display("{stage}")]
pub struct CopyDataStatus {
    pub stage: CopyDataStage,
    pub tables_per_shard: Option<Vec<u64>>,
}

impl From<CopyDataStage> for CopyDataStatus {
    fn from(stage: CopyDataStage) -> Self {
        Self {
            stage,
            tables_per_shard: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CopyDataStage {
    /// Fetching table and column metadata from the source.
    #[display("loading table metadata")]
    LoadingTableMetadata,
    /// Checking that every table has a usable replica identity.
    #[display("validating tables")]
    ValidatingTables,
    /// Creating the replication slots the copy reads from.
    #[display("creating slots")]
    CreatingSlots,
    /// Copying table data to the destination shards.
    #[display("copying tables")]
    CopyingTables,
    /// A stage this build does not know.
    #[display("")]
    #[serde(other)]
    Other,
}

/// The table one copy subtask is copying.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("{schema}.{table}")]
pub struct TableCopyDefinition {
    pub schema: String,
    pub table: String,
    pub source_shard: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TableCopyStage {
    #[display("estimating")]
    Estimation,
    #[display("waiting")]
    WaitingForCopyHandler,
    #[display("copy in progress")]
    InProgress { rows: u64, bytes: u64 },
    #[display("error backoff")]
    ErrorBackoff,
    #[display("")]
    #[serde(other)]
    Other,
}

/// How much of one table has been copied.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[display("{stage}")]
pub struct TableCopyStatus {
    pub stage: TableCopyStage,
    pub attempt: usize,
    pub estimated_rows: Option<u64>,
    pub estimated_bytes: Option<u64>,
    pub rows_per_sec: Option<u64>,
    pub bytes_per_sec: Option<u64>,
    pub last_error: Option<String>,
}
