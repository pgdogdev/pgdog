use pgdog_stats::{AbortReshardingDefinition, TaskDefinition, TaskStatus};
use tracing::info;

use crate::api::Task;
use crate::api::task::TaskContext;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::resharding_state::ReshardingState;
use crate::backend::replication::logical::run::CommandKind;

#[derive(Debug)]
pub(crate) struct AbortReshardingTask {
    state: ReshardingState,
    command: CommandKind,
    slot: Option<String>,
}

impl AbortReshardingTask {
    pub(crate) async fn checked(
        state: ReshardingState,
        command: CommandKind,
        slot: Option<String>,
    ) -> Result<Self, Error> {
        state.check_record(command, slot.as_deref()).await?;
        Ok(Self {
            state,
            command,
            slot,
        })
    }
}

impl Task for AbortReshardingTask {
    type Status = TaskStatus;
    type Output = ();
    type Error = Error;

    fn definition(&self) -> impl Into<TaskDefinition> {
        AbortReshardingDefinition {
            databases: self.state.databases(),
            command: self.command.to_string(),
        }
    }

    async fn run(self, _ctx: TaskContext<Self>) -> Result<(), Error> {
        let record = self
            .state
            .abort_run(self.command, self.slot.as_deref())
            .await?;
        info!("resharding {} aborted", record.key);
        Ok(())
    }
}
