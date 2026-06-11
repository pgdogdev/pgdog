//! Schema-sync background task (pre-data or post-data).

use crate::api::Task;
use crate::api::async_task::AsyncTaskContext;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::orchestrator::Orchestrator;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SchemaSyncPhase {
    Pre,
    Post,
}

/// Stages of a schema sync, reported as the task's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(crate) enum SchemaSyncStatus {
    /// Dumping the schema from the source.
    #[display("loading schema")]
    LoadingSchema,
    /// Restoring tables on the destination (pre-data).
    #[display("syncing tables")]
    SyncingTables,
    /// Creating indexes and constraints on the destination (post-data).
    #[display("creating indexes")]
    CreatingIndexes,
}

/// Sync the schema (pre- or post-data) from a source database to a target.
#[derive(Display, Debug)]
#[display("schema_sync")]
pub(crate) struct SchemaSyncTask {
    pub orchestrator: Orchestrator,
    pub phase: SchemaSyncPhase,
}

impl Task for SchemaSyncTask {
    type Status = SchemaSyncStatus;
    type Output = Orchestrator;
    type Error = Error;

    /// Returns the orchestrator with its schema loaded and synced so a parent
    /// task can thread it into the next phase. The schema dump is skipped when
    /// the orchestrator already carries one (e.g. a parent that runs `Pre`
    /// then `Post` on the same orchestrator).
    async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<Orchestrator, Error> {
        let mut orchestrator = self.orchestrator;

        if orchestrator.schema().is_err() {
            ctx.set_status(SchemaSyncStatus::LoadingSchema);
            orchestrator.load_schema().await?;
        }

        match self.phase {
            SchemaSyncPhase::Pre => {
                ctx.set_status(SchemaSyncStatus::SyncingTables);
                orchestrator.schema_sync_pre(true).await?;
            }
            SchemaSyncPhase::Post => {
                ctx.set_status(SchemaSyncStatus::CreatingIndexes);
                orchestrator.schema_sync_post(true).await?;
            }
        }

        Ok(orchestrator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_sync_status_renders_distinct_labels() {
        let labels = [
            SchemaSyncStatus::LoadingSchema.to_string(),
            SchemaSyncStatus::SyncingTables.to_string(),
            SchemaSyncStatus::CreatingIndexes.to_string(),
        ];
        assert!(labels.iter().all(|label| !label.is_empty()));
        let unique: std::collections::HashSet<_> = labels.iter().collect();
        assert_eq!(unique.len(), labels.len());
    }
}
