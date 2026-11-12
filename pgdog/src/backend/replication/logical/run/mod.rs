use std::collections::HashMap;

use derive_more::{Display, FromStr, FromStrError};

use pgdog_stats::{Databases, InstanceId, RunId, TaskId, run_id};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::Error;
use super::publisher::{Permanent, ReplicationSlot};
use crate::backend::{
    Cluster,
    pool::{Address, Request},
};

mod queries;
pub(crate) mod scan;
pub(crate) mod snapshot;
pub(crate) mod store;
pub(crate) mod watchdog;

pub(crate) use snapshot::ConfigSnapshot;
pub(crate) use store::ReshardingStore;
pub(crate) use watchdog::ActiveRun;

pub(crate) async fn state_address(destination: &Cluster) -> Result<Address, Error> {
    Ok(destination
        .shards()
        .first()
        .ok_or_else(|| Error::NoPrimary)?
        .primary(&Request::default())
        .await?
        .addr()
        .clone())
}

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

/// Defines the command that was ran, with the options of that command.
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

    pub(crate) fn slots_stage(&self) -> Stage {
        match self {
            Self::Reshard {
                replicate_only: false,
                ..
            } => Stage::Copy,
            Self::Reshard { .. } | Self::Replicate { .. } => Stage::Replication,
        }
    }

    pub(crate) fn origins_stage(&self) -> Stage {
        match self {
            Self::Reshard {
                replicate_only: false,
                ..
            } => Stage::TableSync,
            Self::Reshard { .. } | Self::Replicate { .. } => Stage::Replication,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Display)]
#[display(rename_all = "snake_case")]
pub(crate) enum CommandKind {
    Reshard,
    Replicate,
}

/// Specified the last known stage of the resharding execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Display, FromStr)]
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
    Error::ReshardingFormat(err.to_string())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum RunObjects {
    #[default]
    Create,
    Reuse {
        owned: bool,
    },
    Load {
        owned: bool,
    },
}

impl RunObjects {
    pub(crate) fn resumed(stored: Stage, created_at: Stage, owned: bool) -> Self {
        match stored.cmp(&created_at) {
            std::cmp::Ordering::Less => Self::Create,
            std::cmp::Ordering::Equal => Self::Reuse { owned },
            std::cmp::Ordering::Greater => Self::Load { owned },
        }
    }

    pub(crate) fn owned(self) -> bool {
        match self {
            Self::Create => false,
            Self::Reuse { owned } | Self::Load { owned } => owned,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Display, FromStr)]
pub(crate) struct ReshardingId(Uuid);

impl ReshardingId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::now_v7())
    }
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

    pub(crate) fn is_current_process(&self) -> bool {
        self.instance_id.as_str() == crate::util::instance_id() && self.run_id == run_id()
    }
}

/// A single row record for the one resharding process.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReshardingRecord {
    pub(crate) id: ReshardingId,
    pub(crate) key: ReshardingKey,
    pub(crate) command: ReshardingCommand,
    pub(crate) slot_name: String,
    pub(crate) slot_existed: bool,
    pub(crate) stage: Stage,
    pub(crate) config: ConfigSnapshot,
    pub(crate) owner: TaskOwner,
    pub(crate) format_version: i32,
}

pub(crate) async fn existing_slots(
    source: &Cluster,
    publication: &str,
    slot_name: &str,
) -> Result<HashMap<usize, ReplicationSlot<Permanent>>, Error> {
    let mut slots = HashMap::new();
    for (number, shard) in source.shards().iter().enumerate() {
        let addr = shard.primary(&Request::default()).await?.addr().clone();
        let slot =
            ReplicationSlot::new_permanent(publication, &addr, Some(slot_name.to_owned()), number);
        match slot.verify_exists().await {
            Ok(()) => {
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
    fn resumed_objects_follow_the_stage_that_creates_them() {
        let reshard = ReshardingCommand::Reshard {
            skip_schema_sync: false,
            replicate_only: false,
            sync_only: false,
            auto_cutover: false,
            tables_only: false,
        };
        let replicate_only = ReshardingCommand::Reshard {
            skip_schema_sync: true,
            replicate_only: true,
            sync_only: false,
            auto_cutover: false,
            tables_only: false,
        };
        let replicate = ReshardingCommand::Replicate {
            auto_cutover: false,
        };
        let reuse = RunObjects::Reuse { owned: true };
        let load = RunObjects::Load { owned: true };
        let create = RunObjects::Create;

        for (command, stage, slots, origins) in [
            (reshard, Stage::PreData, create, create),
            (reshard, Stage::Copy, reuse, create),
            (reshard, Stage::PostData, load, create),
            (reshard, Stage::TableSync, load, reuse),
            (reshard, Stage::Replication, load, load),
            (replicate_only, Stage::PreData, create, create),
            (replicate_only, Stage::Replication, reuse, reuse),
            (replicate, Stage::Replication, reuse, reuse),
        ] {
            assert_eq!(
                RunObjects::resumed(stage, command.slots_stage(), true),
                slots,
                "{command:?} slots at {stage}"
            );
            assert_eq!(
                RunObjects::resumed(stage, command.origins_stage(), true),
                origins,
                "{command:?} origins at {stage}"
            );
        }
        assert!(!create.owned());
        assert!(!RunObjects::resumed(Stage::Replication, Stage::Copy, false).owned());
    }
}
