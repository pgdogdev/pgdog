//! Reshard task definition and status.

use derive_more::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Databases, ReplicationStatus};

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
/// schema-sync and copy stages live on the child tasks. The reshard task
/// drives the replication itself, so it reports the replication stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReshardStatus {
    /// Running the pre-data schema-sync child task.
    #[display("syncing schema")]
    SchemaSync,
    /// Running the data-copy child task.
    #[display("syncing data")]
    SyncingData,
    /// Running the post-data schema-sync child task.
    #[display("finalizing schema")]
    FinalizingSchema,
    #[display("synchronizing tables")]
    SynchronizingTables,
    /// Replicating; `stage` is the current replication stage.
    #[display("{stage}")]
    Replication {
        #[serde(default)]
        stage: ReplicationStatus,
    },
    /// A stage this build does not know.
    #[display("")]
    #[serde(other)]
    Other,
}

impl From<ReplicationStatus> for ReshardStatus {
    fn from(stage: ReplicationStatus) -> Self {
        Self::Replication { stage }
    }
}
