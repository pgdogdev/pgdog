use std::sync::LazyLock;

use derive_more::{Display, From, FromStr};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const INSTANCE_ID_HEADER: &str = "x-pgdog-instance-id";
pub const RUN_ID_HEADER: &str = "x-pgdog-run-id";

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Display, From, FromStr, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct InstanceId(String);

impl InstanceId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Display,
    From,
    FromStr,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(transparent)]
pub struct RunId(Uuid);

impl RunId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

static RUN_ID: LazyLock<RunId> = LazyLock::new(RunId::generate);

pub fn run_id() -> RunId {
    *RUN_ID
}

/// Identifier of one resharding run, kept across its pauses and resumes.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Display,
    From,
    FromStr,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(transparent)]
pub struct ReshardingId(Uuid);

impl ReshardingId {
    pub fn generate() -> Self {
        Self(Uuid::now_v7())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_run_id_is_stable_within_the_process() {
        assert_eq!(run_id(), run_id());
    }
}
