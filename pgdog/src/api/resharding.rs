//! Reshard / migration composer task.
//!
//! Composes the full migration from a source database to a target.
//! `RESHARD` runs schema sync, data copy, replication, and automatic cutover.
//! The manual admin flow uses `COPY_DATA` without automatic cutover.
//! The CLI `data_sync` command selects stages through its options.

use std::time::Duration;

use tracing::warn;

use crate::api::copy_data::CopyDataTask;
use crate::api::replication::{CutoverMode, ReplicationTask};
use crate::api::schema_sync::{SchemaSyncPhase, SchemaSyncTask};
use crate::api::synchronize_tables::SynchronizeTablesTask;
use crate::api::task::TaskContext;
use crate::api::{ReshardingError, Task, run_task, tasks_storage};
use crate::backend::replication::logical::resharding_state::ReshardingState;
use crate::backend::replication::logical::run::{ActiveRun, ReshardingCommand, Stage};
use crate::config::config;
use pgdog_stats::{ReshardDefinition, ReshardStatus, TaskDefinition};

/// Run the full migration from a source database to a target.
/// Restore pre-data schema, copy data, restore post-data schema, then synchronize tables.
/// With `auto_cutover`, also perform the cutover.
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
    /// Copy only the publication's tables instead of the whole database:
    /// schema sync creates just those tables, referenced tables must exist
    /// on the destination, and replication never cuts over.
    #[builder(default)]
    pub(crate) tables_only: bool,
}

impl Task for ReshardTask {
    type Status = ReshardStatus;
    type Output = ();
    type Error = ReshardingError;

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
            cleanup_on_cancel: self.state.cleanup_on_cancel(),
        }
    }

    async fn run(self, ctx: TaskContext<Self>) -> Result<(), ReshardingError> {
        let cancel = ctx.cancellation_token();
        let cutover = self.cutover_mode();
        let command = self.command();
        let first_stage = self.first_stage();
        let mut state = self.state;
        let root_id = ctx.root_id();
        state.start_run(command, first_stage, root_id).await?;
        let _active = ActiveRun::start(&state, move || {
            tasks_storage().cancel_task(root_id);
        });
        let schema_sync = SchemaSyncTask::builder()
            .publication_tables_only(self.tables_only)
            .databases(state.databases())
            .publication(state.publication.clone());

        // Replicate-only continues an earlier copy.
        let mut copied = self.replicate_only;
        let mut result: Result<(), ReshardingError> = Box::pin(async {
            // Pre-data schema sync, unless skipped. It runs inside the cleanup
            // guard below, so a failure here also reaches the cleanup.
            if !self.skip_schema_sync {
                state.set_stage(Stage::PreData).await?;
                ctx.set_status(ReshardStatus::SchemaSync);
                ctx.run(
                    schema_sync
                        .clone()
                        .phase(SchemaSyncPhase::Pre)
                        .ignore_errors(true)
                        .build(),
                )
                .await?;

                // The pre-data sync changed the destination's schema, so its pools
                // reloaded. `SchemaSync::reload_destination` refreshes only its own
                // cluster refs, so the state still holds stale ones.
                state.reload()?;
            }

            // Copy the data, unless replicate-only.
            if !self.replicate_only {
                ctx.set_status(ReshardStatus::SyncingData);
                ctx.run(
                    CopyDataTask::builder()
                        .state(state.clone())
                        .format(config().config.general.resharding_copy_format)
                        .build(),
                )
                .await?;
                copied = true;
            }

            // Post-data schema sync (secondary indexes, constraints): the
            // second half of schema sync, after the bulk load.
            // It reuses dump schema from the earlier schema_sync
            // calls if they were executed.
            if !self.skip_schema_sync {
                state.set_stage(Stage::PostData).await?;
                ctx.set_status(ReshardStatus::FinalizingSchema);
                ctx.run(
                    schema_sync
                        .clone()
                        .phase(SchemaSyncPhase::Post)
                        .ignore_errors(self.replicate_only)
                        .build(),
                )
                .await?;
            }

            if !self.replicate_only {
                ctx.set_status(ReshardStatus::SynchronizingTables);
                ctx.run(SynchronizeTablesTask {
                    state: state.clone(),
                })
                .await?;
            }

            // Replication, unless sync-only.
            if !self.sync_only {
                // data_sync / schema sync can run for hours; pools may have
                // reloaded. Re-fetch live cluster refs before replicating.
                state.reload()?;

                // The replication reports its stages as this task's status and
                // its subtasks attach to this task. `auto_cutover` (reshard)
                // cuts over on its own; otherwise it runs until an operator
                // `CUTOVER`/`STOP_TASK`. A stop in a forward phase resolves to
                // `Err(ReplicationAborted)` and runs the cleanup below; a stop
                // in a reverse phase resolves to `Ok`, because the migration
                // is already complete.
                Box::pin(
                    ReplicationTask::builder()
                        .state(state.clone())
                        .cutover(cutover)
                        .schema_sync(schema_sync.clone())
                        .validate(!self.skip_schema_sync && !self.replicate_only)
                        .build()
                        .run_inline(&ctx),
                )
                .await?;
            }

            Ok(())
        })
        .await;

        let stopped = result.is_err() || cancel.is_cancelled();
        if stopped {
            // With cleanup_on_cancel true, drop the slots this run created and
            // all its origins, then delete the row. With false, keep them and
            // pause the run. A slot that existed before the run always stays.
            if let Err(err) = state.cancel_run().await {
                warn!("failed to clean up the resharding run: {err}");
            }
        } else {
            // on success we either already removed slots on replication,
            // or replication was not called and we should leave the
            // slots for the future calls
            state.detach_slots();
            if let Err(err) = state.finish_run().await {
                result = Err(err.into());
            }
        }

        if self.tables_only && copied {
            // There's no cutover, so run the cutover phase here to set the
            // tables' sequences. It's a separate task because a stop cancels
            // this one and every subtask.
            let sequences = run_task(
                schema_sync
                    .phase(SchemaSyncPhase::Cutover)
                    .ignore_errors(true)
                    .build(),
            )
            .await;
            if let Err(err) = sequences {
                warn!("failed to sync sequences: {err}");
            }
        }

        result
    }
}

impl ReshardTask {
    /// Copying only some tables never cuts over: a cutover swaps whole databases.
    fn cutover_mode(&self) -> CutoverMode {
        if self.tables_only {
            CutoverMode::Disabled
        } else if self.auto_cutover {
            CutoverMode::Auto
        } else {
            CutoverMode::Manual
        }
    }

    fn first_stage(&self) -> Stage {
        if !self.skip_schema_sync {
            Stage::PreData
        } else if !self.replicate_only {
            Stage::Copy
        } else {
            Stage::Replication
        }
    }

    fn command(&self) -> ReshardingCommand {
        ReshardingCommand::Reshard {
            skip_schema_sync: self.skip_schema_sync,
            replicate_only: self.replicate_only,
            sync_only: self.sync_only,
            auto_cutover: self.auto_cutover,
            tables_only: self.tables_only,
        }
    }
}
