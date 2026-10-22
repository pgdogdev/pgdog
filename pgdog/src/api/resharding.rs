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
use crate::api::{MigrationError, Task};
use crate::backend::replication::logical::resharding_state::ReshardingState;
use crate::backend::replication::logical::table_move::TableMove;
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
    /// Move this single table instead of the tables in a user publication.
    /// Schema sync covers only this table, and replication never cuts over.
    pub(crate) table_move: Option<TableMove>,
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
        let cutover = self.cutover_mode();
        let mut state = self.state;
        let schema_sync = SchemaSyncTask::builder()
            .databases(state.databases())
            .publication(state.publication.clone())
            .publication_tables_only(self.table_move.is_some());

        if let Some(table_move) = &self.table_move {
            table_move.prepare(&state).await?;
        }

        // Pre-data schema sync, unless skipped. It runs before any replication
        // slots exist, so it stays outside the cleanup guard below.
        let prepared: Result<(), MigrationError> = Box::pin(async {
            if !self.skip_schema_sync {
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

            if let Some(table_move) = &self.table_move {
                table_move.check_table(&state.destination).await?;
            }

            Ok(())
        })
        .await;

        if let Err(err) = prepared {
            if let Some(table_move) = &self.table_move {
                table_move.finish(&state, false).await;
            }
            return Err(err);
        }

        let mut copied = false;
        let result: Result<(), MigrationError> = Box::pin(async {
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
                ctx.set_status(ReshardStatus::Replication);

                // data_sync / schema sync can run for hours; pools may have
                // reloaded. Re-fetch live cluster refs before replicating.
                state.reload()?;

                // `auto_cutover` (reshard) cuts over on its own; otherwise the
                // task runs until an operator `CUTOVER`/`STOP_TASK`. A stop in
                // a forward phase resolves to `Err(ReplicationAborted)` and runs
                // the cleanup below; a stop in a reverse phase resolves to
                // `Ok`, because the migration is already complete.
                Box::pin(
                    ctx.run(
                        ReplicationTask::builder()
                            .state(state.clone())
                            .cutover(cutover)
                            .schema_sync(schema_sync.clone())
                            .validate(!self.skip_schema_sync && !self.replicate_only)
                            .build(),
                    ),
                )
                .await?;
            }

            Ok(())
        })
        .await;

        // A table move drops its publication, so its slots can't be reused.
        if result.is_err() || cancel.is_cancelled() || self.table_move.is_some() {
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

        if let Some(table_move) = &self.table_move {
            table_move.finish(&state, copied).await;
        }

        result
    }
}

impl ReshardTask {
    /// A table move never cuts over: a cutover swaps whole databases.
    fn cutover_mode(&self) -> CutoverMode {
        if self.table_move.is_some() {
            CutoverMode::Disabled
        } else if self.auto_cutover {
            CutoverMode::Auto
        } else {
            CutoverMode::Manual
        }
    }
}
