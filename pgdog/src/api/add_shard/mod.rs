//! Add-shard background task.
//!
//! Provisions a new shard for a cluster whose sharded tables are
//! placement-stable (`lookup_result = "shard"` or explicit mappings):
//! syncs DDL, snapshot-copies omnisharded tables from shard 0, streams
//! WAL until the new shard has caught up, then — on operator `CUTOVER`
//! or automatically — pauses omnisharded writes fleet-wide, drains
//! replication to zero, activates the shard in the topology, and
//! resumes. Sharded traffic and all reads flow throughout; only omni
//! writes pause, for the sub-second drain.
//!
//! The destination is the shard declared with `provisioning = true` in
//! the config: declared in its final shape, excluded from the serving
//! topology until the cutover flips the flag in the running config.
//!
//! Each phase lives in its own module with its contract documented:
//! [`guards`] (everything the task holds), [`provision`] (schema,
//! data, catch-up), [`cutover`] (park, drain, swap, finalize), and
//! [`schema_only`] (the degenerate path without omnisharded tables).

mod cutover;
mod guards;
mod provision;
mod schema_only;

use std::time::Duration;

use crate::api::async_task::AsyncTaskContext;
use crate::api::{MigrationError, Task};
use crate::backend::replication::logical::orchestrator::Orchestrator;

use guards::Preflight;
use provision::Publication;

/// Stages of adding a shard, reported as the task's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(crate) enum AddShardStatus {
    /// Checking that the cluster can grow in place.
    #[display("validating")]
    Validating,
    /// Running the pre-data schema-sync child task.
    #[display("syncing schema")]
    SchemaSync,
    /// Running the data-copy child task.
    #[display("syncing data")]
    SyncingData,
    /// Running the post-data schema-sync child task.
    #[display("finalizing schema")]
    FinalizingSchema,
    /// Streaming changes to catch the new shard up.
    #[display("replicating")]
    Replicating,
    /// Caught up; waiting for an operator `CUTOVER`.
    #[display("awaiting cutover")]
    AwaitingCutover,
    /// Omni writes paused; draining replication to zero.
    #[display("draining")]
    Draining,
    /// Swapping the new shard into the topology.
    #[display("swapping topology")]
    SwappingTopology,
}

/// Outcome of a cutover attempt.
enum CutoverOutcome {
    /// The shard is in the topology.
    Done,
    /// The cutover was called off (drain timeout, or the fleet wasn't
    /// ready): every barrier was released and replication continues;
    /// the task goes back to waiting for a cutover.
    Aborted,
}

/// Add a new shard to a database: provision the shard declared with
/// `provisioning = true` in the config, catch it up over logical
/// replication, and activate it in the topology.
#[derive(Display, Debug, bon::Builder)]
#[display("add_shard {database} shard {shard}")]
pub(crate) struct AddShardTask {
    /// The database gaining a shard.
    pub database: String,
    /// The shard being added: names one of the database's
    /// `provisioning = true` entries.
    pub shard: usize,
    /// Operator-supplied publication; when absent, one is created for
    /// the omnisharded tables and dropped when the task ends.
    pub publication: Option<String>,
    /// Cut over automatically once the new shard has caught up,
    /// instead of waiting for an operator `CUTOVER`.
    #[builder(default)]
    pub auto_cutover: bool,
}

impl Task for AddShardTask {
    type Status = AddShardStatus;
    type Output = ();
    type Error = MigrationError;

    fn cancel_timeout() -> Duration {
        Duration::from_secs(60)
    }

    async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<(), MigrationError> {
        // Take the cancellation token so a `STOP_TASK` winds the
        // children down cooperatively.
        let token = ctx.cancellation_token();

        // Refuse anything that would corrupt or collide, and take
        // every lock the task holds until it ends.
        ctx.set_status(AddShardStatus::Validating);
        let preflight = guards::preflight(&self).await?;

        // With no omnisharded tables there is nothing to copy or
        // stream, and no writes to pause: sync the schema and activate.
        if preflight.omni_tables.is_empty() {
            return schema_only::run(&self, &ctx, &token, &preflight).await;
        }

        // Publication for the omni tables; dropped when the task ends
        // unless the operator owns it.
        let publication = Publication::ensure(&self, &preflight).await?;
        let orchestrator = Orchestrator::for_provisioning(
            &self.database,
            preflight.destination().clone(),
            publication.name(),
            0,
        )?;

        let result = self
            .provision_and_cutover(&ctx, &token, orchestrator, &preflight)
            .await;

        publication.cleanup(&self, result.is_ok()).await;
        result
    }
}

impl AddShardTask {
    /// Schema, data, catch-up, then the cutover loop: each pass waits
    /// for catch-up, parks for the operator (unless auto), and
    /// attempts the cutover; an aborted attempt re-enters at catch-up.
    /// Replication slots are cleaned up on any error past the pre-data
    /// schema sync.
    async fn provision_and_cutover(
        &self,
        ctx: &AsyncTaskContext<Self>,
        token: &tokio_util::sync::CancellationToken,
        orchestrator: Orchestrator,
        preflight: &Preflight,
    ) -> Result<(), MigrationError> {
        // DDL first: no replication slots exist yet, so this stays
        // outside the cleanup guard.
        let orchestrator = provision::sync_schema_pre(self, ctx, orchestrator).await?;

        // From here the orchestrator may hold replication slots.
        let slots = orchestrator.publication_guard();
        let result: Result<(), MigrationError> = async {
            let orchestrator = provision::copy_data(self, ctx, orchestrator).await?;
            let orchestrator = provision::sync_schema_post(self, ctx, orchestrator).await?;
            let mut waiter = provision::replicate(self, ctx, orchestrator).await?;

            // Catch up, then cut over: automatically, or when the
            // operator says so.
            loop {
                provision::wait_for_catch_up(self, token, &waiter).await?;

                // A cancellation during catch-up stops the task. It
                // must not fall through into the cutover: auto mode
                // has no other cancellation check before the swap.
                if token.is_cancelled() {
                    return provision::stop_stream(&mut waiter).await;
                }

                if !self.auto_cutover {
                    match cutover::park(self, ctx, token, &mut waiter).await? {
                        cutover::Parked::CutoverRequested => {}
                        cutover::Parked::Ended => return Ok(()),
                    }
                }

                match cutover::run(self, ctx, &mut waiter, preflight).await? {
                    CutoverOutcome::Done => return Ok(()),
                    CutoverOutcome::Aborted => {
                        ctx.set_status(AddShardStatus::Replicating);
                    }
                }
            }
        }
        .await;

        if result.is_err()
            && let Err(err) = slots.cleanup().await
        {
            tracing::warn!("failed to clean up replication slots after add shard: {err}");
        }

        result
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_status_labels_distinct() {
        let statuses = [
            AddShardStatus::Validating,
            AddShardStatus::SchemaSync,
            AddShardStatus::SyncingData,
            AddShardStatus::FinalizingSchema,
            AddShardStatus::Replicating,
            AddShardStatus::AwaitingCutover,
            AddShardStatus::Draining,
            AddShardStatus::SwappingTopology,
        ];
        let labels: HashSet<String> = statuses.iter().map(|s| s.to_string()).collect();
        assert_eq!(labels.len(), statuses.len());
        assert!(labels.iter().all(|label| !label.is_empty()));
    }
}
