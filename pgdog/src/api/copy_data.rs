//! Copy-data leaf task: bulk-copies table data from a source to a target.
//!
//! This task only copies data. The schema sync (pre-data tables, post-data
//! indexes) and replication around it are composed by
//! [`ReshardTask`](crate::api::resharding::ReshardTask).

use tokio::select;

use crate::api::Task;
use crate::api::async_task::{AsyncTaskContext, Empty};
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::orchestrator::Orchestrator;

/// Bulk-copy table data from a source database to a target, returning the
/// orchestrator so the composing task can thread it into the next phase.
#[derive(Display, Debug, bon::Builder)]
#[display("copy_data {orchestrator}")]
pub(crate) struct CopyDataTask {
    pub orchestrator: Orchestrator,
}

impl Task for CopyDataTask {
    type Status = Empty;
    type Output = Orchestrator;
    type Error = Error;

    async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<Orchestrator, Error> {
        let token = ctx.cancellation_token();
        let orchestrator = self.orchestrator;

        select! {
            res = orchestrator.data_sync() => res?,
            // Cancellation drops the `data_sync()` future, whose internal
            // `JoinSet` aborts every in-flight shard copy; closing those
            // connections releases the temporary data-sync slots. The
            // composing task drops the persistent replication slots afterward.
            _ = token.cancelled() => return Err(Error::DataSyncAborted),
        }

        Ok(orchestrator)
    }
}
