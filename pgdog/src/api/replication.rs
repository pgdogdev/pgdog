//! Logical-replication background task.
//!
//! Drives a `ReplicationWaiter` to completion. Without `auto_cutover`
//! (standalone `REPLICATE`, `copy_data`) it stops on cancellation
//! (`STOP_TASK`), cuts over on an operator `CUTOVER` addressed to this task
//! (delivered through [`ReplicationTask::cutover`]), and otherwise finishes
//! when the source slot drains (no cutover on natural drain). With
//! `auto_cutover` set (reshard) it cuts over automatically once the
//! destination has caught up.

use std::time::Duration;

use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::api::Task;
use crate::api::async_task::AsyncTaskContext;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::orchestrator::ReplicationWaiter;

/// Stages of logical replication, reported as the task's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(crate) enum ReplicationStatus {
    /// Streaming changes to catch the destination up.
    #[display("replicating")]
    Replicating,
    /// Cutting traffic over to the destination.
    #[display("cutting over")]
    CuttingOver,
    /// Cutting traffic back to the original after a prior cutover (rollback).
    #[display("rolling back")]
    RollingBack,
    /// Winding down on a stop request.
    #[display("stopping")]
    Stopping,
}

/// Direction of a replication task: the initial migration (`Forward`) or the
/// post-cutover reverse stream that backs a rollback (`Reverse`). A `CUTOVER`
/// on a `Reverse` task is therefore a rollback. Affects reported status only,
/// not control flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Direction {
    #[default]
    Forward,
    Reverse,
}

/// Run the replication by driving a [`ReplicationWaiter`] to completion.
#[derive(Display, Debug, bon::Builder)]
#[display("replication {waiter}{}", match direction {
    Direction::Forward => "",
    Direction::Reverse => " (reverse)",
})]
pub(crate) struct ReplicationTask {
    /// The running replication waiter this task drives to completion.
    pub waiter: ReplicationWaiter,
    /// Cut over automatically once the destination has caught up, instead
    /// of waiting for an operator `CUTOVER`.
    #[builder(default)]
    pub auto_cutover: bool,
    /// Replication direction. `Reverse` marks the post-cutover stream that
    /// backs a rollback; it only affects reported status, not control flow.
    #[builder(default)]
    pub direction: Direction,
}

impl Task for ReplicationTask {
    type Status = ReplicationStatus;
    type Output = ();
    type Error = Error;

    fn cancel_timeout() -> Duration {
        Duration::from_secs(60)
    }

    async fn run(mut self, ctx: AsyncTaskContext<Self>) -> Result<(), Error> {
        let token = ctx.cancellation_token();

        ctx.set_status(ReplicationStatus::Replicating);

        if self.auto_cutover {
            return self.perform_cutover(&ctx, &token).await;
        }

        let cutover = crate::api::cutover_registry::register_cutover(
            ctx.root_id(),
            &self.waiter.source_database(),
            None,
        );

        select! {
            _ = token.cancelled() => {
                ctx.set_status(ReplicationStatus::Stopping);
                self.waiter.stop();
            }
            _ = cutover.requested() => {
                self.perform_cutover(&ctx, &token).await?;
            }
            res = self.waiter.wait() => {
                res?;
            }
        }

        Ok(())
    }
}

impl ReplicationTask {
    /// Perform the actual cutover for running replication.
    async fn perform_cutover(
        &mut self,
        ctx: &AsyncTaskContext<Self>,
        token: &CancellationToken,
    ) -> Result<(), Error> {
        ctx.set_status(match self.direction {
            Direction::Forward => ReplicationStatus::CuttingOver,
            Direction::Reverse => ReplicationStatus::RollingBack,
        });
        self.waiter.cutover(token).await
    }
}
