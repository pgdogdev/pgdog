use derive_more::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ReplicationProgress;

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
