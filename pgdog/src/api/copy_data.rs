//! Copy-data leaf task: bulk-copies table data from a source to a target.
//!
//! This task only copies data. The schema sync (pre-data tables, post-data
//! indexes) and replication around it are composed by
//! [`ReshardTask`](crate::api::resharding::ReshardTask).

use crate::api::Task;
use crate::api::task::TaskContext;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::orchestrator::Orchestrator;
use pgdog_stats::{CopyDataDefinition, CopyDataStatus, TaskDefinition};

/// Bulk-copy table data from a source database to a target.
#[derive(Debug, bon::Builder)]
pub(crate) struct CopyDataTask {
    pub(crate) orchestrator: Orchestrator,
    /// Require a usable replica identity per table. Only streaming needs it,
    /// so a sync-only migration passes `false`. See `Publisher::data_sync`.
    pub(crate) require_replica_identity: bool,
}

impl Task for CopyDataTask {
    type Status = CopyDataStatus;
    type Output = ();
    type Error = Error;

    fn definition(&self) -> impl Into<TaskDefinition> {
        CopyDataDefinition {
            databases: self.orchestrator.databases(),
        }
    }

    async fn run(self, ctx: TaskContext<Self>) -> Result<(), Error> {
        let token = ctx.cancellation_token();

        // Don't start a sync that's already cancelled. Once it's running, the
        // token is threaded into the copy workers, which abort their COPY loops
        // on cancellation; the composing task drops the slots afterward.
        if token.is_cancelled() {
            return Err(Error::DataSyncAborted);
        }

        self.orchestrator
            .data_sync(&token, self.require_replica_identity)
            .await
    }
}
