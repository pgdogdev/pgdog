use std::collections::HashMap;
use std::time::SystemTime;

use derive_more::{Display, FromStr, FromStrError};

use pgdog_stats::{CommandKind, Databases, InstanceId, ReshardingId, RunId, TaskId, run_id};
use serde::{Deserialize, Serialize};

use super::Error;
use super::publisher::{Permanent, ReplicationSlot};
use crate::backend::{
    Cluster,
    pool::{Address, LbRequest},
};

mod queries;
pub(crate) mod scan;
pub(crate) mod snapshot;
mod store;

pub(crate) use snapshot::ConfigSnapshot;
pub(crate) use store::ReshardingStore;

pub(crate) async fn state_address(destination: &Cluster) -> Result<Address, Error> {
    Ok(destination
        .shards()
        .first()
        .ok_or_else(|| Error::NoPrimary)?
        .primary(&LbRequest::default())
        .await?
        .addr()
        .clone())
}

pub(crate) const RESHARDING_TABLE: &str = "resharding";

/// Version of the data in [`RESHARDING_TABLE`]. Increase it on breaking changes.
pub(crate) const FORMAT_VERSION: i32 = 1;

/// What a resharding command does with its run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunMode {
    /// Start a run. With `cleanup_on_cancel` false, a stop pauses it.
    Run { cleanup_on_cancel: bool },
    /// Abort the unfinished run with the same key.
    Abort,
    /// Resume the paused or crashed run with the same key.
    Resume,
}

impl Default for RunMode {
    fn default() -> Self {
        Self::Run {
            cleanup_on_cancel: true,
        }
    }
}

impl RunMode {
    pub(crate) fn cleanup_on_cancel(self) -> bool {
        matches!(
            self,
            Self::Abort
                | Self::Run {
                    cleanup_on_cancel: true
                }
        )
    }
}

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

/// The command that started the run, with its options.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Display, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub(crate) enum ReshardingCommand {
    #[display("reshard")]
    Reshard {
        skip_schema_sync: bool,
        replicate_only: bool,
        sync_only: bool,
        auto_cutover: bool,
        tables_only: bool,
    },
    #[display("replicate")]
    Replicate { auto_cutover: bool },
}

impl ReshardingCommand {
    pub(crate) fn kind(&self) -> CommandKind {
        match self {
            Self::Reshard { .. } => CommandKind::Reshard,
            Self::Replicate { .. } => CommandKind::Replicate,
        }
    }
}

/// The last known stage of the resharding run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Display, FromStr)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[from_str(error(Error, format_error))]
pub(crate) enum Stage {
    Initializing,
    CopyTables,
    Replication,
}

fn format_error(err: FromStrError) -> Error {
    Error::ReshardingFormat(err.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq, Display)]
#[display("task {task_id} on {instance_id} (run {run_id})")]
pub(crate) struct TaskOwner {
    pub(crate) instance_id: InstanceId,
    pub(crate) run_id: RunId,
    pub(crate) task_id: TaskId,
}

impl TaskOwner {
    pub(crate) fn current(task_id: TaskId) -> Self {
        Self {
            instance_id: InstanceId::from(crate::util::instance_id().to_owned()),
            run_id: run_id(),
            task_id,
        }
    }
}

/// A single row record for the one resharding process.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReshardingRecord {
    pub(crate) id: ReshardingId,
    pub(crate) key: ReshardingKey,
    pub(crate) command: ReshardingCommand,
    pub(crate) slot_name: String,
    pub(crate) stage: Stage,
    pub(crate) config: ConfigSnapshot,
    pub(crate) owner: TaskOwner,
    pub(crate) format_version: i32,
    pub(crate) started_at: SystemTime,
    pub(crate) resumed_at: Option<SystemTime>,
}

pub(crate) async fn existing_slots(
    source: &Cluster,
    publication: &str,
    slot_name: &str,
) -> Result<HashMap<usize, ReplicationSlot<Permanent>>, Error> {
    let mut slots = HashMap::new();
    for (number, shard) in source.shards().iter().enumerate() {
        let addr = shard.primary(&LbRequest::default()).await?.addr().clone();
        let slot =
            ReplicationSlot::new_permanent(publication, &addr, Some(slot_name.to_owned()), number);
        match slot.verify_exists().await {
            Ok(()) | Err(Error::ReplicationSlotWalLost(_)) => {
                slots.insert(number, slot);
            }
            Err(Error::MissingReplicationSlot(_)) => (),
            Err(err) => return Err(err),
        }
    }
    Ok(slots)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn stage_round_trips_its_stored_text_in_pipeline_order() {
        let stages = [
            (Stage::Initializing, "initializing"),
            (Stage::CopyTables, "copy_tables"),
            (Stage::Replication, "replication"),
        ];
        for (stage, text) in stages {
            assert_eq!(stage.to_string(), text);
            assert_eq!(text.parse::<Stage>().unwrap(), stage);
        }
        for pair in stages.windows(2) {
            assert!(pair[0].0 < pair[1].0, "{} < {}", pair[0].1, pair[1].1);
        }
        assert!(matches!(
            "new_stage".parse::<Stage>(),
            Err(Error::ReshardingFormat(_))
        ));
    }
}
