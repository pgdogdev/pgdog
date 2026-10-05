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

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct Resharding {
    #[serde(default)]
    pub post_data_validation: PostDataValidationStage,
}
