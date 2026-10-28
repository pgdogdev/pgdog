use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Serialize,
    Deserialize,
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Default,
    JsonSchema,
    derive_more::Display,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[display(rename_all = "snake_case")]
pub enum PostDataValidationStage {
    Off,
    #[default]
    DuringReplication,
    BeforeCutover,
    AfterCutover,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct Resharding {
    #[serde(default)]
    pub post_data_validation: PostDataValidationStage,

    /// How long, in milliseconds, a table copy waits for a failed 2PC commit
    /// to be committed or rolled back. On timeout, the copy fails.
    /// _Default:_ `600000` (10 minutes)
    #[serde(default = "Resharding::two_pc_cleanup_timeout")]
    pub two_pc_cleanup_timeout: u64,
}

impl Resharding {
    fn two_pc_cleanup_timeout() -> u64 {
        600_000
    }
}

impl Default for Resharding {
    fn default() -> Self {
        Self {
            post_data_validation: PostDataValidationStage::default(),
            two_pc_cleanup_timeout: Self::two_pc_cleanup_timeout(),
        }
    }
}
