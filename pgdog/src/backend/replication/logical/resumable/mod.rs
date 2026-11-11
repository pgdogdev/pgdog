use std::sync::Arc;

use derive_more::{Display, FromStr, FromStrError};

use pgdog_stats::Databases;
use serde::{Deserialize, Serialize};

use super::Error;

mod queries;
pub(crate) mod snapshot;
pub(crate) mod store;

pub(crate) use snapshot::ConfigSnapshot;
pub(crate) use store::ReshardingStore;

pub(crate) const RESHARDING_TABLE: &str = "resharding";

/// Version of the data in [`RESHARDING_TABLE`]. Need to be update in case of breaking changes
pub(crate) const FORMAT_VERSION: i32 = 1;

/// Identifier of the specific resharding.
#[derive(Clone, Debug, Hash, Eq, PartialEq, Display)]
#[display("{databases} ({publication})")]
pub(crate) struct ReshardingKey {
    pub(crate) databases: Databases,
    pub(crate) publication: String,
}

impl ReshardingKey {
    pub(crate) fn new(databases: Databases, publication: &str) -> Self {
        Self {
            databases,
            publication: publication.to_owned(),
        }
    }
}

/// Defines the command that was ran
#[derive(Clone, Copy, Debug, PartialEq, Eq, Display, FromStr)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[from_str(error(Error, format_error))]
pub(crate) enum ReshardingCommand {
    Reshard,
    Replicate,
}

/// Specified the last known stage of the resharding execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Display, FromStr)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[from_str(error(Error, format_error))]
pub(crate) enum Stage {
    PreData,
    Copy,
    PostData,
    TableSync,
    Replication,
    Cutover,
    ReverseReplication,
}

fn format_error(err: FromStrError) -> Error {
    Error::ResumableFormat(err.to_string())
}

/// Save the flags that was specified with the command to run resharding
/// in order to be able to catch inconsistencies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CommandFlags {
    pub(crate) skip_schema_sync: bool,
    pub(crate) replicate_only: bool,
    pub(crate) sync_only: bool,
    pub(crate) auto_cutover: bool,
    pub(crate) tables_only: bool,
}

/// A single row record for the one resharding process.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReshardingRecord {
    pub(crate) key: ReshardingKey,
    pub(crate) command: ReshardingCommand,
    pub(crate) command_flags: CommandFlags,
    pub(crate) slot_name: String,
    pub(crate) slot_existed: bool,
    pub(crate) stage: Stage,
    pub(crate) config: ConfigSnapshot,
    pub(crate) format_version: i32,
}

#[derive(Debug, Default)]
pub(crate) enum ResumableState {
    #[default]
    No,
    Active(Arc<ReshardingStore>),
    Paused,
    Finished,
}

impl ResumableState {
    pub(crate) fn keeps_slots(&self) -> bool {
        matches!(self, Self::Active(_) | Self::Paused)
    }
}
