use derive_more::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Databases, ReplicationProgress};

/// The tables one synchronize-tables task brings to a common LSN after the
/// bulk copy.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("synchronize_tables {databases}")]
pub struct SynchronizeTablesDefinition {
    pub databases: Databases,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SynchronizeTablesStatus {
    #[display("initializing replication streams")]
    InitializingReplicationStreams,
    #[display("synchronizing tables, {progress}")]
    SynchronizingTables { progress: ReplicationProgress },
    #[display("")]
    #[serde(other)]
    Other,
}
