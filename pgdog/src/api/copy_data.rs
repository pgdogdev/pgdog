//! Copy-data background task: schema sync + data sync, then a replication
//! task that catches up and (on `CUTOVER`) cuts over.

use crate::api::async_task::AsyncTaskContext;
use crate::api::replication::ReplicationTask;
use crate::api::schema_sync::{SchemaSyncPhase, SchemaSyncTask};
use crate::api::{MigrationError, Task};
use crate::backend::replication::logical::orchestrator::Orchestrator;

/// Copy data from a source database to a target: schema sync, data sync,
/// then replication catch-up and cutover.
#[derive(Display, Debug)]
#[display("copy_data")]
pub(crate) struct CopyDataTask {
    pub orchestrator: Orchestrator,
}

/// Stages of the copy-data flow, reported as the task's status. The
/// fine-grained schema-sync and replication stages live on the child tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(crate) enum CopyDataStatus {
    /// Running the schema-sync child task.
    #[display("syncing schema")]
    SchemaSync,
    /// Copying table data to the destination.
    #[display("syncing data")]
    SyncingData,
    /// Running the replication child task.
    #[display("replicating")]
    Replication,
}

impl Task for CopyDataTask {
    type Status = CopyDataStatus;
    type Output = ();
    type Error = MigrationError;

    async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<(), MigrationError> {
        // Sync the schema as a child task so it reports its own stages.
        ctx.set_status(CopyDataStatus::SchemaSync);
        let mut orchestrator = ctx
            .run(SchemaSyncTask {
                orchestrator: self.orchestrator,
                phase: SchemaSyncPhase::Pre,
            })
            .await?;

        ctx.set_status(CopyDataStatus::SyncingData);
        orchestrator.data_sync().await?;

        // data_sync can run for hours; pools may have reloaded. Re-fetch
        // live cluster refs before starting replication.
        orchestrator.refresh()?;

        // Replication runs as a child until cutover, reporting its own stages.
        // Awaiting keeps copy_data non-terminal while it runs; its outcome is
        // intentionally not propagated here.
        ctx.set_status(CopyDataStatus::Replication);
        let _ = ctx.run(ReplicationTask { orchestrator }).await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_data_status_renders_distinct_labels() {
        let labels = [
            CopyDataStatus::SchemaSync.to_string(),
            CopyDataStatus::SyncingData.to_string(),
            CopyDataStatus::Replication.to_string(),
        ];
        assert!(labels.iter().all(|label| !label.is_empty()));
        let unique: std::collections::HashSet<_> = labels.iter().collect();
        assert_eq!(unique.len(), labels.len());
    }
}
