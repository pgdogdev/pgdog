//! Logical-replication background task.
//!
//! Drives a `ReplicationWaiter` to completion. Without `auto_cutover`
//! (standalone `REPLICATE`, `copy_data`) it stops on cancellation
//! (`STOP_TASK`), cuts over on an operator `CUTOVER` addressed to this task
//! (delivered through [`ReplicationTask::cutover`]), and otherwise finishes
//! when the source slot drains (no cutover on natural drain). With
//! `auto_cutover` set (reshard) it cuts over automatically once the
//! destination has caught up.

use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::api::Task;
use crate::api::async_task::{AsyncTaskContext, AsyncTaskId};
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

/// Cutover tokens of the replication tasks currently awaiting an operator
/// `CUTOVER`, keyed by the root task id they belong to. A cutover token is
/// *separate* from the task's `STOP_TASK` cancellation token — signalling it
/// means "cut over", not "abandon".
static CUTOVERS: LazyLock<Mutex<HashMap<AsyncTaskId, CancellationToken>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Guard held by a running replication task: removes its cutover
/// registration on drop. Awaiting [CutoverWaiter::requested]
/// resolves when an operator `CUTOVER` targets the task.
struct CutoverWaiter {
    root_id: AsyncTaskId,
    token: CancellationToken,
}

impl CutoverWaiter {
    /// Wait until a cutover is requested for this task. The token latches, so
    /// a cutover that arrived earlier is delivered immediately.
    async fn requested(&self) {
        self.token.cancelled().await;
    }
}

impl Drop for CutoverWaiter {
    fn drop(&mut self) {
        CUTOVERS.lock().remove(&self.root_id);
    }
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

        let cutover = Self::register_cutover(ctx.root_id());

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

    /// Trigger a cutover on a running replication task.
    pub(crate) fn trigger_cutover(target: Option<AsyncTaskId>) -> bool {
        let tokens = CUTOVERS.lock();

        let token = match target {
            Some(id) => tokens.get(&id),
            // No id: cut over the first (lowest-id) running task.
            None => tokens.keys().min().and_then(|id| tokens.get(id)),
        };

        match token {
            Some(token) => {
                token.cancel();
                true
            }
            None => false,
        }
    }

    /// Register this task (by its `root_id`) to receive operator cutovers for
    /// as long as the returned guard is held.
    fn register_cutover(root_id: AsyncTaskId) -> CutoverWaiter {
        let token = CancellationToken::new();
        CUTOVERS.lock().insert(root_id, token.clone());
        CutoverWaiter { root_id, token }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    // Serialize tests that touch the process-global `CUTOVERS` map so they
    // never observe each other's registrations under a multi-threaded harness.
    static CUTOVER_TEST_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

    #[tokio::test]
    async fn cutover_delivers_even_when_buffered() {
        let _guard = CUTOVER_TEST_LOCK.lock().await;
        // Cutover lands before the task awaits: still delivered (latches).
        let waiter = ReplicationTask::register_cutover(AsyncTaskId::from(1));
        assert!(
            ReplicationTask::trigger_cutover(Some(AsyncTaskId::from(1))),
            "the named task must receive the cutover"
        );

        tokio::time::timeout(Duration::from_secs(1), waiter.requested())
            .await
            .expect("buffered cutover was not delivered");
    }

    #[tokio::test]
    async fn cutover_targets_only_the_named_task() {
        let _guard = CUTOVER_TEST_LOCK.lock().await;
        // A cutover for one id must never disturb a task registered under a
        // different id — the whole point of keying by task id.
        let waiter = ReplicationTask::register_cutover(AsyncTaskId::from(7));

        assert!(
            !ReplicationTask::trigger_cutover(Some(AsyncTaskId::from(8))),
            "no task is registered under id 8"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(200), waiter.requested())
                .await
                .is_err(),
            "a cutover for a different id leaked to this task"
        );

        assert!(ReplicationTask::trigger_cutover(Some(AsyncTaskId::from(7))));
        tokio::time::timeout(Duration::from_secs(1), waiter.requested())
            .await
            .expect("targeted cutover was not delivered");
    }

    #[tokio::test]
    async fn cutover_without_id_targets_the_first_task() {
        let _guard = CUTOVER_TEST_LOCK.lock().await;
        // No id: the lowest-id (first) registered task is cut over, and only
        // it.
        let first = ReplicationTask::register_cutover(AsyncTaskId::from(3));
        let second = ReplicationTask::register_cutover(AsyncTaskId::from(9));

        assert!(
            ReplicationTask::trigger_cutover(None),
            "the first registered task must be cut over"
        );

        tokio::time::timeout(Duration::from_secs(1), first.requested())
            .await
            .expect("the first task was not cut over");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), second.requested())
                .await
                .is_err(),
            "cutover(None) disturbed a task other than the first"
        );
    }

    #[tokio::test]
    async fn cutover_does_not_leak_to_the_next_task() {
        let _guard = CUTOVER_TEST_LOCK.lock().await;
        // A cutover to a task that never consumes it must die with that task,
        // never reaching the next one. Regression guard for the signal leak.
        {
            let first = ReplicationTask::register_cutover(AsyncTaskId::from(1));
            assert!(ReplicationTask::trigger_cutover(Some(AsyncTaskId::from(1))));
            drop(first); // ends without ever awaiting `requested()`
        }

        let next = ReplicationTask::register_cutover(AsyncTaskId::from(2));
        assert!(
            tokio::time::timeout(Duration::from_millis(200), next.requested())
                .await
                .is_err(),
            "stale cutover leaked into the next replication task"
        );
    }

    #[tokio::test]
    async fn cutover_with_no_task_is_rejected() {
        let _guard = CUTOVER_TEST_LOCK.lock().await;
        // Nothing registered: `CUTOVER` (with or without an id) is rejected.
        assert!(!ReplicationTask::trigger_cutover(None));
        assert!(!ReplicationTask::trigger_cutover(Some(AsyncTaskId::from(
            404
        ))));
    }
}
