//! Reshard background task: the full automatic schema-sync + data-sync +
//! replication + cutover flow.

use crate::api::async_task::AsyncTaskContext;
use crate::api::replication::ReplicationTask;
use crate::api::schema_sync::{SchemaSyncPhase, SchemaSyncTask};
use crate::api::{MigrationError, Task};
use crate::backend::replication::logical::cutover_signal;
use crate::backend::replication::logical::orchestrator::Orchestrator;

/// Run the complete replicate-and-cutover flow from a source database to a
/// target.
#[derive(Display, Debug)]
#[display("reshard")]
pub(crate) struct ReshardTask {
    pub orchestrator: Orchestrator,
}

/// Stages of the reshard flow, reported as the task's status. The
/// fine-grained schema-sync and replication stages live on the child tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(crate) enum ReshardStatus {
    /// Running the pre-data schema-sync child task.
    #[display("syncing schema")]
    SchemaSync,
    /// Copying table data to the destination.
    #[display("syncing data")]
    SyncingData,
    /// Running the post-data schema-sync child task (indexes, constraints).
    #[display("finalizing schema")]
    FinalizingSchema,
    /// Running the replication child task through cutover.
    #[display("replicating")]
    Replication,
}

impl Task for ReshardTask {
    type Status = ReshardStatus;
    type Output = ();
    type Error = MigrationError;

    async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<(), MigrationError> {
        // Sync the pre-data schema (tables) as a child task.
        ctx.set_status(ReshardStatus::SchemaSync);
        let orchestrator = ctx
            .run(SchemaSyncTask {
                orchestrator: self.orchestrator,
                phase: SchemaSyncPhase::Pre,
            })
            .await?;

        // Sync the data to destination.
        ctx.set_status(ReshardStatus::SyncingData);
        orchestrator.data_sync().await?;

        // Create secondary indexes as a child task (schema already loaded).
        ctx.set_status(ReshardStatus::FinalizingSchema);
        let mut orchestrator = ctx
            .run(SchemaSyncTask {
                orchestrator,
                phase: SchemaSyncPhase::Post,
            })
            .await?;

        // Refresh cluster references: data_sync can take hours and the pools
        // may have been reloaded (e.g. by a client DDL) in the meantime.
        orchestrator.refresh()?;

        // Reshard cuts over automatically: request it up front (the cutover
        // signal is buffered), then run replication as a child that consumes
        // the request and cuts over once it has caught up.
        ctx.set_status(ReshardStatus::Replication);
        cutover_signal::request();
        ctx.run(ReplicationTask { orchestrator }).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reshard_status_renders_distinct_labels() {
        let labels = [
            ReshardStatus::SchemaSync.to_string(),
            ReshardStatus::SyncingData.to_string(),
            ReshardStatus::FinalizingSchema.to_string(),
            ReshardStatus::Replication.to_string(),
        ];
        assert!(labels.iter().all(|label| !label.is_empty()));
        let unique: std::collections::HashSet<_> = labels.iter().collect();
        assert_eq!(unique.len(), labels.len());
    }
}
