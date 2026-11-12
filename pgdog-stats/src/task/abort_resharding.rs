//! Abort resharding task definition and status.

use derive_more::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Databases;

/// Abort of the unfinished resharding with the key (source, destination,
/// publication) that `command` started.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("abort {command} {databases}")]
pub struct AbortReshardingDefinition {
    pub databases: Databases,
    pub command: String,
}
