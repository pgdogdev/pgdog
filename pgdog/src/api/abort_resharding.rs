use std::time::Duration;

use pgdog_stats::{AbortReshardingDefinition, TaskDefinition, TaskStatus};
use tokio::time::{Instant, sleep};
use tracing::info;

use crate::api::task::TaskContext;
use crate::api::{Task, tasks_storage};
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::resharding_state::ReshardingState;
use crate::backend::replication::logical::run::{CommandKind, ReshardingRecord, ReshardingStore};

const ABORT_STOP_TIMEOUT: Duration = Duration::from_secs(90);
const ABORT_STOP_POLL: Duration = Duration::from_millis(100);

#[derive(Debug, bon::Builder)]
pub(crate) struct AbortReshardingTask {
    state: ReshardingState,
    command: CommandKind,
    slot: Option<String>,
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
        let (store, record) = self.stop_and_open().await?;
        self.state.adopt_run(&record).await?;
        self.state.drop_slots().await?;
        store.delete().await?;
        info!("resharding {} aborted", record.key);
        Ok(())
    }
}

impl AbortReshardingTask {
    async fn stop_and_open(&self) -> Result<(ReshardingStore, ReshardingRecord), Error> {
        let slot = self.slot.as_deref();
        let deadline = Instant::now() + ABORT_STOP_TIMEOUT;
        let mut cancelled = false;
        loop {
            match self.state.open_for_abort(self.command, slot).await {
                Err(Error::ReshardingLocked { key, owner }) => {
                    if !cancelled {
                        let record = self.state.check_record(self.command, slot).await?;
                        if !record.owner.is_current_process() {
                            return Err(Error::ReshardingLocked { key, owner });
                        }
                        tasks_storage().cancel_task(record.owner.task_id);
                        cancelled = true;
                    }
                    if Instant::now() >= deadline {
                        return Err(Error::ReshardingBusy(key));
                    }
                    sleep(ABORT_STOP_POLL).await;
                }
                opened => return opened,
            }
        }
    }
}
