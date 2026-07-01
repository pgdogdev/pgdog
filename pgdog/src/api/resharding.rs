//! Reshard / migration composer task.
//!
//! Composes the full migration from a source database to a target: pre-data
//! schema sync, the [`CopyDataTask`] bulk copy, post-data schema sync, then
//! replication. With `auto_cutover` it cuts over automatically once replication
//! has caught up (reshard); otherwise it waits for an operator `CUTOVER`
//! (`copy_data`). The admin `COPY_DATA`/`RESHARD` commands and the CLI
//! `data_sync` all run this task, differing only in their options.

use std::time::Duration;

use tracing::warn;

use crate::api::async_task::AsyncTaskContext;
use crate::api::copy_data::CopyDataTask;
use crate::api::replication::ReplicationTask;
use crate::api::schema_sync::{SchemaSyncPhase, SchemaSyncTask};
use crate::api::{MigrationError, Task};
use crate::backend::replication::logical::orchestrator::Orchestrator;

/// Run the full migration from a source database to a target: schema sync
/// (pre-data tables, then post-data indexes around the bulk copy), data copy,
/// then replication. With `auto_cutover` it also performs the cutover.
#[derive(Display, Debug, bon::Builder)]
#[display("reshard {orchestrator}")]
pub(crate) struct ReshardTask {
    pub orchestrator: Orchestrator,
    /// Skip the pre- and post-data schema sync.
    #[builder(default)]
    pub skip_schema_sync: bool,
    /// Only replicate; skip the initial data copy.
    #[builder(default)]
    pub replicate_only: bool,
    /// Only copy data; skip replication.
    #[builder(default)]
    pub sync_only: bool,
    /// Cut over automatically once replication has caught up, instead of
    /// waiting for an operator `CUTOVER`. Set by the reshard flow.
    #[builder(default)]
    pub auto_cutover: bool,
}

/// Stages of the migration, reported as the task's status. The fine-grained
/// schema-sync, copy, and replication stages live on the child tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(crate) enum ReshardStatus {
    /// Running the pre-data schema-sync child task.
    #[display("syncing schema")]
    SchemaSync,
    /// Running the data-copy child task.
    #[display("syncing data")]
    SyncingData,
    /// Running the post-data schema-sync child task (indexes, constraints).
    #[display("finalizing schema")]
    FinalizingSchema,
    /// Running the replication child task.
    #[display("replicating")]
    Replication,
}

impl Task for ReshardTask {
    type Status = ReshardStatus;
    type Output = ();
    type Error = MigrationError;

    fn cancel_timeout() -> Duration {
        Duration::from_secs(60)
    }

    async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<(), MigrationError> {
        // Take the cancellation token so a `STOP_TASK` winds the children down
        // cooperatively (they'd otherwise outlive this task).
        let _token = ctx.cancellation_token();
        let mut orchestrator = self.orchestrator;

        // Pre-data schema sync, unless skipped. It runs before any replication
        // slots exist, so it stays outside the cleanup guard below.
        if !self.skip_schema_sync {
            ctx.set_status(ReshardStatus::SchemaSync);
            orchestrator = ctx
                .run(
                    SchemaSyncTask::builder()
                        .orchestrator(orchestrator)
                        .phase(SchemaSyncPhase::Pre)
                        .ignore_errors(true)
                        .build(),
                )
                .await?;
        }

        // From the data copy onward the orchestrator may hold replication slots
        // (created during data_sync, kept until replication takes them over).
        // Awaiting this guard on every exit drops whatever the publisher still
        // owns — a no-op once replication has claimed the slots — so a failed or
        // aborted migration doesn't leave them lingering on the source.
        let guard = orchestrator.publication_guard();
        let result: Result<(), MigrationError> = async {
            // Copy the data, unless replicate-only.
            if !self.replicate_only {
                ctx.set_status(ReshardStatus::SyncingData);
                orchestrator = ctx
                    .run(CopyDataTask::builder().orchestrator(orchestrator).build())
                    .await?;
            }

            // Post-data schema sync (secondary indexes, constraints): the
            // second half of schema sync, after the bulk load.
            if !self.skip_schema_sync {
                ctx.set_status(ReshardStatus::FinalizingSchema);

                // The bulk copy above can run for hours; pools may have reloaded
                // meanwhile, leaving our cluster refs stale. Re-fetch them before
                // touching the destination.
                orchestrator.refresh()?;

                orchestrator = ctx
                    .run(
                        SchemaSyncTask::builder()
                            .orchestrator(orchestrator)
                            .phase(SchemaSyncPhase::Post)
                            .ignore_errors(true)
                            .build(),
                    )
                    .await?;
            }

            // Replication, unless sync-only.
            if !self.sync_only {
                ctx.set_status(ReshardStatus::Replication);

                // data_sync / schema sync can run for hours; pools may have
                // reloaded. Re-fetch live cluster refs before replicating.
                orchestrator.refresh()?;

                // `auto_cutover` (reshard) cuts over on its own; otherwise the
                // task runs until an operator `CUTOVER`/`STOP_TASK`. Both of
                // those resolve to `Ok`, so awaiting surfaces only a genuine
                // replication failure.
                let waiter = orchestrator.replicate().await?;
                ctx.run(
                    ReplicationTask::builder()
                        .waiter(waiter)
                        .auto_cutover(self.auto_cutover)
                        .build(),
                )
                .await?;
            }

            Ok(())
        }
        .await;

        // Drop any replication slots the publisher still owns only when the
        // migration failed or was aborted mid-copy.
        if result.is_err()
            && let Err(err) = guard.cleanup().await
        {
            warn!("failed to clean up replication slots after migration: {err}");
        }

        result
    }
}
