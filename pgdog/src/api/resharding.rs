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

use crate::api::copy_data::CopyDataTask;
use crate::api::replication::ReplicationTask;
use crate::api::schema_sync::{SchemaSyncPhase, SchemaSyncTask};
use crate::api::task::TaskContext;
use crate::api::{MigrationError, Task};
use crate::backend::replication::logical::resharding_state::ReshardingState;
use crate::config::config;
use pgdog_stats::{ReshardDefinition, ReshardStatus, TaskDefinition};

/// Run the full migration from a source database to a target: schema sync
/// (pre-data tables, then post-data indexes around the bulk copy), data copy,
/// then replication. With `auto_cutover` it also performs the cutover.
#[derive(Debug, bon::Builder)]
pub(crate) struct ReshardTask {
    pub(crate) state: ReshardingState,
    /// Skip the pre- and post-data schema sync.
    #[builder(default)]
    pub(crate) skip_schema_sync: bool,
    /// Only replicate; skip the initial data copy.
    #[builder(default)]
    pub(crate) replicate_only: bool,
    /// Only copy data; skip replication.
    #[builder(default)]
    pub(crate) sync_only: bool,
    /// Cut over automatically once replication has caught up, instead of
    /// waiting for an operator `CUTOVER`. Set by the reshard flow.
    #[builder(default)]
    pub(crate) auto_cutover: bool,
}

impl Task for ReshardTask {
    type Status = ReshardStatus;
    type Output = ();
    type Error = MigrationError;

    fn cancel_timeout() -> Duration {
        Duration::from_secs(60)
    }

    fn definition(&self) -> impl Into<TaskDefinition> {
        ReshardDefinition {
            databases: self.state.databases(),
            skip_schema_sync: self.skip_schema_sync,
            replicate_only: self.replicate_only,
            sync_only: self.sync_only,
            auto_cutover: self.auto_cutover,
        }
    }

    async fn run(self, ctx: TaskContext<Self>) -> Result<(), MigrationError> {
        let cancel = ctx.cancellation_token();
        let mut state = self.state;
        let schema_sync = SchemaSyncTask::builder()
            .databases(state.databases())
            .publication(state.publication.clone())
            .ignore_errors(true);

        // Pre-data schema sync, unless skipped. It runs before any replication
        // slots exist, so it stays outside the cleanup guard below.
        if !self.skip_schema_sync {
            ctx.set_status(ReshardStatus::SchemaSync);
            ctx.run(schema_sync.clone().phase(SchemaSyncPhase::Pre).build())
                .await?;

            // The pre-data sync changed the destination's schema, so its pools
            // reloaded. `SchemaSync::reload_destination` refreshes only its own
            // cluster refs, so the state still holds stale ones.
            state.reload()?;
        }

        let result: Result<(), MigrationError> = async {
            // Copy the data, unless replicate-only.
            if !self.replicate_only {
                ctx.set_status(ReshardStatus::SyncingData);
                ctx.run(
                    CopyDataTask::builder()
                        .state(state.clone())
                        .format(config().config.general.resharding_copy_format)
                        // Only streaming needs replica identity, not a sync-only copy.
                        .require_replica_identity(!self.sync_only)
                        .build(),
                )
                .await?;
            }

            // Post-data schema sync (secondary indexes, constraints): the
            // second half of schema sync, after the bulk load.
            // It reuses dump schema from the earlier schema_sync
            // calls if they were executed.
            if !self.skip_schema_sync {
                ctx.set_status(ReshardStatus::FinalizingSchema);
                ctx.run(schema_sync.clone().phase(SchemaSyncPhase::Post).build())
                    .await?;
            }

            // Replication, unless sync-only.
            if !self.sync_only {
                ctx.set_status(ReshardStatus::Replication);

                // data_sync / schema sync can run for hours; pools may have
                // reloaded. Re-fetch live cluster refs before replicating.
                state.reload()?;

                // `auto_cutover` (reshard) cuts over on its own; otherwise the
                // task runs until an operator `CUTOVER`/`STOP_TASK`. A stop in
                // a forward phase resolves to `Err(ReplicationAborted)` and runs
                // the cleanup below; a stop in a reverse phase resolves to
                // `Ok`, because the migration is already complete.
                ctx.run(
                    ReplicationTask::builder()
                        .state(state.clone())
                        .auto_cutover(self.auto_cutover)
                        .schema_sync(schema_sync.clone().phase(SchemaSyncPhase::Cutover).build())
                        .build(),
                )
                .await?;
            }

            Ok(())
        }
        .await;

        if result.is_err() || cancel.is_cancelled() {
            // on error or cancellation make sure we drop the slots,
            // but only the slots we're actually created during this run.
            // If the slots were created outside or by calling pgdog
            // separate runs, we leave the slots.
            if let Err(err) = state.drop_slots_if_owned().await {
                warn!("failed to clean up replication slots after migration: {err}");
            }
        } else {
            // on success we either already removed slots on replication,
            // or replication was not called and we should leave the
            // slots for the future calls
            state.detach_slots();
        }

        result
    }
}
