//! Abort resharding task definition.

use derive_more::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Databases;

#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("abort {command} {databases}")]
pub struct AbortReshardingDefinition {
    pub databases: Databases,
    pub command: String,
}
