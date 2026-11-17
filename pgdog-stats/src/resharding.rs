//! Resharding types shared with the control plane.

use derive_more::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The kind of command that started a resharding run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, Serialize, Deserialize, JsonSchema)]
#[display(rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum CommandKind {
    Reshard,
    Replicate,
    /// A kind this build does not know, sent by a newer pgdog.
    #[serde(other)]
    Unknown,
}
