//! Reshard task definition and status.

use derive_more::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Databases;

/// The full migration one reshard task runs, and which phases it was asked
/// to skip.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("reshard {databases}")]
pub struct ReshardDefinition {
    pub databases: Databases,
    pub skip_schema_sync: bool,
    pub replicate_only: bool,
    pub sync_only: bool,
    pub auto_cutover: bool,
}

/// Stages of the migration, reported as the task's status. The fine-grained
/// schema-sync, copy, and replication stages live on the child tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReshardStatus {
    /// Running the pre-data schema-sync child task.
    #[display("syncing schema")]
    SchemaSync,
    /// Running the data-copy child task.
    #[display("syncing data")]
    SyncingData,
    /// Running the post-data schema-sync child task (indexes, constraints).
    #[display("finalizing schema")]
    FinalizingSchema,
    /// Running the replication child task.
    #[display("replicating")]
    Replication,
    /// A stage this build does not know.
    #[display("")]
    #[serde(other)]
    Other,
}
