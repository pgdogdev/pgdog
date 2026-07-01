//! Schema-sync background task (pre-data, post-data, or cutover).

use std::ops::Deref;

use crate::api::Task;
use crate::api::async_task::AsyncTaskContext;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::orchestrator::Orchestrator;
use crate::backend::schema::sync::pg_dump::SyncState;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Display, FromStr)]
pub enum SchemaSyncPhase {
    #[display("pre")]
    Pre,
    #[display("post")]
    Post,
    #[display("cutover")]
    Cutover,
}

impl From<SchemaSyncPhase> for SyncState {
    fn from(phase: SchemaSyncPhase) -> Self {
        match phase {
            SchemaSyncPhase::Pre => SyncState::PreData,
            SchemaSyncPhase::Post => SyncState::PostData,
            SchemaSyncPhase::Cutover => SyncState::Cutover,
        }
    }
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
    /// Restoring cutover-time schema on the destination.
    #[display("syncing cutover schema")]
    Cutover,
}

/// Sync the schema (pre-data, post-data, or cutover) from a source database to a target.
#[derive(Display, Debug, bon::Builder)]
#[display("schema_sync({phase}) {orchestrator}")]
pub(crate) struct SchemaSyncTask {
    pub orchestrator: Orchestrator,
    pub phase: SchemaSyncPhase,
    #[builder(default)]
    pub ignore_errors: bool,
    #[builder(default)]
    pub dry_run: bool,
}

impl Task for SchemaSyncTask {
    type Status = SchemaSyncStatus;
    type Output = Orchestrator;
    type Error = Error;

    #[allow(clippy::print_stdout)]
    async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<Orchestrator, Error> {
        let mut orchestrator = self.orchestrator;

        if orchestrator.schema().is_err() {
            ctx.set_status(SchemaSyncStatus::LoadingSchema);
            orchestrator.load_schema().await?;
        }

        if self.dry_run {
            let schema = orchestrator.schema()?;
            for statement in schema.statements(self.phase.into())? {
                println!("{}", statement.deref());
            }
            return Ok(orchestrator);
        }

        match self.phase {
            SchemaSyncPhase::Pre => {
                ctx.set_status(SchemaSyncStatus::SyncingTables);
                orchestrator.schema_sync_pre(self.ignore_errors).await?;
            }
            SchemaSyncPhase::Post => {
                ctx.set_status(SchemaSyncStatus::CreatingIndexes);
                orchestrator.schema_sync_post(self.ignore_errors).await?;
            }
            SchemaSyncPhase::Cutover => {
                ctx.set_status(SchemaSyncStatus::Cutover);
                orchestrator.schema_sync_cutover(self.ignore_errors).await?;
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
            SchemaSyncStatus::Cutover.to_string(),
        ];
        assert!(labels.iter().all(|label| !label.is_empty()));
        let unique: std::collections::HashSet<_> = labels.iter().collect();
        assert_eq!(unique.len(), labels.len());
    }

    #[test]
    fn schema_sync_phase_parses_and_displays() {
        for (text, phase) in [
            ("pre", SchemaSyncPhase::Pre),
            ("post", SchemaSyncPhase::Post),
            ("cutover", SchemaSyncPhase::Cutover),
        ] {
            assert_eq!(text.parse::<SchemaSyncPhase>().unwrap(), phase);
            assert_eq!(phase.to_string(), text);
        }
        // Parsing is case-insensitive; unknown phases are rejected.
        assert_eq!(
            "CUTOVER".parse::<SchemaSyncPhase>().unwrap(),
            SchemaSyncPhase::Cutover
        );
        assert!("bogus".parse::<SchemaSyncPhase>().is_err());
    }
}
