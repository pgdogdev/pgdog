//! Logical-replication background task.
//!
//! Drives a `ReplicationWaiter` to completion: it stops on cancellation
//! (`STOP_TASK`), performs cutover on an external `cutover_signal::request()`
//! (`CUTOVER`), and otherwise finishes when the source slot drains (no cutover
//! on natural drain). Launch it top-level with [`super::start`], or as a child
//! by spawning it through a parent task's [`AsyncTaskContext`].

use std::time::Duration;

use tokio::select;

use crate::api::Task;
use crate::api::async_task::AsyncTaskContext;
use crate::backend::replication::logical::orchestrator::Orchestrator;
use crate::backend::replication::logical::{Error, cutover_signal};

/// Stages of logical replication, reported as the task's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(crate) enum ReplicationStatus {
    /// Streaming changes to catch the destination up.
    #[display("replicating")]
    Replicating,
    /// Cutting traffic over to the destination.
    #[display("cutting over")]
    CuttingOver,
    /// Winding down on a stop request.
    #[display("stopping")]
    Stopping,
}

/// Replicate from a source database to a target, owning the orchestrator
/// that produces the replication waiter.
#[derive(Display, Debug)]
#[display("replication")]
pub(crate) struct ReplicationTask {
    pub orchestrator: Orchestrator,
}

impl Task for ReplicationTask {
    type Status = ReplicationStatus;
    type Output = ();
    type Error = Error;

    /// A cutover, once started, must run to completion. `STOP_TASK` during
    /// the waiting phase is handled by the `select!` arm below (graceful
    /// `waiter.stop()`); a `STOP_TASK` during an in-flight `waiter.cutover()`
    /// waits out this (effectively unbounded) grace period instead of
    /// force-aborting mid-cutover.
    fn cancel_timeout() -> Duration {
        Duration::from_secs(24 * 60 * 60)
    }

    async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<(), Error> {
        let mut waiter = self.orchestrator.replicate().await?;
        let token = ctx.cancellation_token();

        ctx.set_status(ReplicationStatus::Replicating);

        select! {
            _ = token.cancelled() => {
                ctx.set_status(ReplicationStatus::Stopping);
                waiter.stop();
            }
            _ = cutover_signal::requested() => {
                ctx.set_status(ReplicationStatus::CuttingOver);
                waiter.cutover().await?;
            }
            res = waiter.wait() => {
                res?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_timeout_far_exceeds_default() {
        // Far larger than the 5s default: cutover must not be force-aborted.
        assert!(ReplicationTask::cancel_timeout() > Duration::from_secs(60));
    }

    #[test]
    fn replication_status_renders_distinct_labels() {
        let labels = [
            ReplicationStatus::Replicating.to_string(),
            ReplicationStatus::CuttingOver.to_string(),
            ReplicationStatus::Stopping.to_string(),
        ];
        assert!(labels.iter().all(|label| !label.is_empty()));
        let unique: std::collections::HashSet<_> = labels.iter().collect();
        assert_eq!(unique.len(), labels.len());
    }
}
