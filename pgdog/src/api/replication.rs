use std::sync::LazyLock;
use std::time::Duration;

use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use crate::api::Task;
use crate::api::schema_sync::{SchemaSyncPhase, SchemaSyncTask};
use crate::api::task::{TaskContext, TaskId};
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::orchestrator::Orchestrator;
use crate::backend::replication::logical::publisher::cutover_policy::CutoverPolicy;
use crate::backend::replication::logical::publisher::replication_progress::ReplicationProgress;
use crate::backend::replication::logical::publisher::replication_stream::ReplicationStream;
use crate::backend::replication::logical::publisher::{ReplicationSlot, Table};
use crate::backend::{
    databases::{cancel_all, cutover},
    maintenance_mode,
};
use crate::config::config;
use crate::util::{safe_interval, safe_timeout};
use pgdog_stats::{
    Lsn, MissedRows, ReplicationClusterDefinition, ReplicationClusterStatus,
    ReplicationCutoverReason, ReplicationDefinition, ReplicationDirection,
    ReplicationShardDefinition, ReplicationShardStatus, ReplicationStatus, TaskDefinition,
};
use tracing::warn;

#[derive(Debug, bon::Builder)]
pub(crate) struct ReplicationTask {
    pub(crate) orchestrator: Orchestrator,
    /// Cut over automatically once the destination has caught up, instead
    /// of waiting for an operator `CUTOVER`.
    #[builder(default)]
    pub(crate) auto_cutover: bool,
    pub(crate) schema_sync: SchemaSyncTask,
}

macro_rules! return_if_cancelled {
    ($ctx:expr, $direction:expr) => {
        if $ctx.cancellation_token().is_cancelled() {
            return match $direction {
                // if it's forward direction, the cancellation means this was actually cancelled
                ReplicationDirection::Forward => Err(Error::DataSyncAborted),
                // and for reverse application the cancellation means we cancel the reverse replication
                // and the whole replication process was successful
                ReplicationDirection::Reverse => Ok(()),
            };
        }
    };
}

impl Task for ReplicationTask {
    type Status = ReplicationStatus;
    type Output = ();
    type Error = Error;

    fn cancel_timeout() -> Duration {
        // the cancellation should be handled by the task itself,
        // TODO: though, some stages are not cancellable at all,
        // so maybe this should be dynamic?
        Duration::from_secs(600)
    }

    fn definition(&self) -> impl Into<TaskDefinition> {
        ReplicationDefinition {
            databases: self.orchestrator.databases(),
            auto_cutover: self.auto_cutover,
        }
    }

    async fn run(self, ctx: TaskContext<Self>) -> Result<(), Error> {
        let Self {
            mut orchestrator,
            mut schema_sync,
            auto_cutover,
        } = self;
        // we always start with forward direction and then create the reverse
        // direction, and reverse(reverse) = forward
        let mut direction = ReplicationDirection::Forward;
        // W: maybe simplier?
        let mut maintenance =
            Self::replicate_until_cutover(&ctx, &orchestrator, direction, auto_cutover).await?;

        loop {
            return_if_cancelled!(ctx, direction);
            ctx.set_status(ReplicationStatus::SyncingSchema);
            ctx.run(schema_sync).await?;

            // start the cutover only if there is no errors so far
            // and the task is not canceled.
            // after that we can't cancel the task.
            return_if_cancelled!(ctx, direction);
            Self::cutover(&ctx, &mut orchestrator, direction).await?;
            maintenance.resume_traffic();

            return_if_cancelled!(ctx, direction);
            maintenance =
                Self::replicate_until_cutover(&ctx, &orchestrator, direction, false).await?;
            schema_sync = SchemaSyncTask::builder()
                .databases(orchestrator.databases())
                .publication(orchestrator.publication.clone())
                .phase(SchemaSyncPhase::Cutover)
                .ignore_errors(true)
                .build();
            direction = match direction {
                ReplicationDirection::Forward => ReplicationDirection::Reverse,
                ReplicationDirection::Reverse => ReplicationDirection::Forward,
            };
        }
    }
}

impl ReplicationTask {
    async fn replicate_until_cutover(
        ctx: &TaskContext<Self>,
        orchestrator: &Orchestrator,
        direction: ReplicationDirection,
        auto_cutover: bool,
    ) -> Result<MaintenanceMode, Error> {
        let task_cancel = ctx.cancellation_token();
        let cutover = Self::register_cutover(ctx.root_id());
        let progress = ReplicationProgress::new(orchestrator.source.shards().len());
        let (cluster, stop_cluster_replication) =
            ReplicationClusterTask::new(orchestrator.clone(), direction, progress.clone());
        let mut maintenance = MaintenanceMode::new();
        let mut cutover_reason = None;
        ctx.set_status(ReplicationStatus::Replicating);
        let cluster_run = ctx.run(cluster);
        tokio::pin!(cluster_run);
        let result = select! {
            biased;
            _ = task_cancel.cancelled() => Ok(()),
            result = &mut cluster_run => {
                result?;
                // error, since this should not happen, without our cutover signaling
                return Err(Error::ReplicationStreamStopped);
            }
            result = async {
                if !auto_cutover {
                    cutover.requested().await;
                }
                Self::prepare_cutover(ctx, orchestrator, progress, &mut maintenance).await
            } => result.map(|reason| cutover_reason = Some(reason)),
        };

        // stop the cluster replication and wait until it gracefully finishes,
        // we should stop it despite if we succeed or not at this moment
        stop_cluster_replication.stop(cutover_reason);
        let drained = safe_timeout(Self::cancel_timeout(), &mut cluster_run)
            .await
            .unwrap_or(Err(Error::ReplicationTimeout));
        result.and(drained)?;

        Ok(maintenance)
    }

    async fn cutover(
        ctx: &TaskContext<Self>,
        orchestrator: &mut Orchestrator,
        direction: ReplicationDirection,
    ) -> Result<(), Error> {
        ctx.set_status(ReplicationStatus::PreparingReverseReplication);
        orchestrator.refresh_publisher();
        let guard = orchestrator.publication_guard();
        let result = async {
            orchestrator
                .publisher()
                .await
                // W: should we cancellation token from ctx?
                .create_slots(&orchestrator.destination, &CancellationToken::new())
                .await?;
            ctx.set_status(match direction {
                ReplicationDirection::Forward => ReplicationStatus::CuttingOver,
                ReplicationDirection::Reverse => ReplicationStatus::RollingBack,
            });
            cutover(
                &orchestrator.source.identifier().database,
                &orchestrator.destination.identifier().database,
            )
            .await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            if let Err(cleanup) = guard.cleanup().await {
                warn!("failed to clean up reverse replication slots: {cleanup}");
            }
            return result;
        }
        orchestrator.refresh()
    }

    async fn prepare_cutover(
        ctx: &TaskContext<Self>,
        orchestrator: &Orchestrator,
        progress: ReplicationProgress,
        maintenance: &mut MaintenanceMode,
    ) -> Result<ReplicationCutoverReason, Error> {
        let cutover_policy = CutoverPolicy::new(config().as_ref().into(), progress);
        cutover_policy.wait_for_stop_threshold().await?;
        ctx.set_status(ReplicationStatus::StoppingTraffic);
        maintenance.stop_traffic();
        cancel_all(&orchestrator.source.identifier().database).await?;
        ctx.set_status(ReplicationStatus::WaitingForCatchUp);
        cutover_policy.wait_for_catchup().await
    }

    /// Trigger a cutover on a running replication task.
    pub(crate) fn trigger_cutover(target: Option<TaskId>) -> bool {
        let token = match target {
            Some(id) => CUTOVERS.get(&id).map(|entry| entry.value().clone()),
            // No id: cut over the first (lowest-id) running task.
            None => CUTOVERS
                .iter()
                .min_by_key(|entry| *entry.key())
                .map(|entry| entry.value().clone()),
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
    fn register_cutover(root_id: TaskId) -> CutoverWaiter {
        let token = CancellationToken::new();
        CUTOVERS.insert(root_id, token.clone());
        CutoverWaiter { root_id, token }
    }
}

/// Handle that stops one replication cluster, with the cutover reason when
/// the parent task stopped it to cut traffic over.
#[derive(Debug)]
pub(crate) struct ReplicationClusterStop {
    sender: tokio::sync::oneshot::Sender<Option<ReplicationCutoverReason>>,
}

impl ReplicationClusterStop {
    fn stop(self, cutover_reason: Option<ReplicationCutoverReason>) {
        let _ = self.sender.send(cutover_reason);
    }
}

#[derive(Debug)]
pub(crate) struct ReplicationClusterTask {
    orchestrator: Orchestrator,
    progress: ReplicationProgress,
    direction: ReplicationDirection,
    stop: tokio::sync::oneshot::Receiver<Option<ReplicationCutoverReason>>,
}

impl ReplicationClusterTask {
    fn new(
        orchestrator: Orchestrator,
        direction: ReplicationDirection,
        progress: ReplicationProgress,
    ) -> (Self, ReplicationClusterStop) {
        let (sender, stop) = tokio::sync::oneshot::channel();
        (
            Self {
                orchestrator,
                progress,
                direction,
                stop,
            },
            ReplicationClusterStop { sender },
        )
    }
}

impl Task for ReplicationClusterTask {
    type Status = ReplicationClusterStatus;
    type Output = ();
    type Error = Error;

    fn cancel_timeout() -> Duration {
        Duration::from_secs(600)
    }

    fn definition(&self) -> impl Into<TaskDefinition> {
        ReplicationClusterDefinition {
            databases: self.orchestrator.databases(),
            direction: self.direction,
        }
    }

    async fn run(self, ctx: TaskContext<Self>) -> Result<(), Error> {
        let Self {
            orchestrator,
            progress,
            stop,
            ..
        } = self;
        let task_cancel = ctx.cancellation_token();
        let mut streams = ReplicationStreams::new();
        let streams_stop = CancellationToken::new();
        let guard = orchestrator.publication_guard();

        ctx.set_status(ReplicationClusterStatus::InitializingReplicationStreams);
        let init_result = Self::create_replication_stream_tasks(
            &orchestrator,
            &ctx,
            &progress,
            &streams_stop,
            &mut streams,
        )
        .await;

        let mut report = safe_interval(Duration::from_secs(1));
        let mut stop = stop;
        let result = async {
            init_result?;
            loop {
                ctx.set_status(ReplicationClusterStatus::Replicating {
                    progress: progress.snapshot(),
                });
                select! {
                    biased;
                    _ = task_cancel.cancelled() => return Ok(()),
                    stopped = &mut stop => {
                        if let Ok(Some(reason)) = stopped {
                            ctx.set_status(ReplicationClusterStatus::StoppedForCutover { reason });
                        }
                        return Ok(());
                    }
                    // if any of streams exit early, stop the process
                    result = streams.next() => {
                        if let Some(child) = result {
                            child??;
                        }
                        return Err(Error::ReplicationStreamStopped);
                    }
                    _ = report.tick() => {}
                }
            }
        }
        .await;

        // stop all the stream and make sure they are drained.
        // If there were error on some stream it should stop other streams.
        streams_stop.cancel();
        let drained = Self::drain_streams(&mut streams).await;
        let cleanup = guard.cleanup().await;
        result.and(drained).and(cleanup)
    }
}

impl ReplicationClusterTask {
    async fn create_replication_stream_tasks(
        orchestrator: &Orchestrator,
        ctx: &TaskContext<Self>,
        progress: &ReplicationProgress,
        stop: &CancellationToken,
        streams: &mut ReplicationStreams,
    ) -> Result<(), Error> {
        let mut publisher = orchestrator.publisher().await;
        publisher
            .prepare_replication(&orchestrator.source, &ctx.cancellation_token())
            .await?;
        for source_shard in 0..orchestrator.source.shards().len() {
            let tables = publisher.pop_tables(source_shard)?;
            let slot = publisher.pop_slot(source_shard)?;
            let updater = progress.updater_for_shard(source_shard);
            let replication_stream =
                ReplicationStream::new(&orchestrator.source, &orchestrator.destination, updater);
            let task = ReplicationShardTask::builder()
                .source_shard(source_shard)
                .slot(slot)
                .tables(tables)
                .replication_stream(replication_stream)
                .stop(stop.clone())
                .build();
            let child = ctx.run(task);
            streams.push(AbortOnDropHandle::new(tokio::spawn(child)));
        }
        Ok(())
    }

    /// Drain all the streams - make sure they are drained and generated no errors
    async fn drain_streams(streams: &mut ReplicationStreams) -> Result<(), Error> {
        let result = safe_timeout(Self::cancel_timeout(), async {
            let mut result = Ok(());
            while let Some(child) = streams.next().await {
                result = result.and(child.map_err(Error::from).and_then(|result| result));
            }
            result
        })
        .await
        .unwrap_or(Err(Error::ReplicationTimeout));
        if result.is_err() {
            streams.clear();
        }
        result
    }
}

#[derive(Debug, bon::Builder)]
pub(crate) struct ReplicationShardTask {
    pub(crate) slot: ReplicationSlot,
    pub(crate) source_shard: usize,
    pub(crate) tables: Vec<Table>,
    pub(crate) replication_stream: ReplicationStream,
    pub(crate) stop: CancellationToken,
}

impl Task for ReplicationShardTask {
    type Status = ReplicationShardStatus;
    type Output = ();
    type Error = Error;

    fn cancel_timeout() -> Duration {
        Duration::from_secs(60)
    }

    fn definition(&self) -> impl Into<TaskDefinition> {
        ReplicationShardDefinition {
            slot: self.slot.name().to_owned(),
            host: self.slot.addr().host.clone(),
            port: self.slot.addr().port,
            database_name: self.slot.addr().database_name.clone(),
            source_shard: self.source_shard,
        }
    }

    async fn run(self, ctx: TaskContext<Self>) -> Result<(), Error> {
        let Self {
            slot,
            tables,
            replication_stream,
            stop,
            ..
        } = self;

        // task got cancelled
        let task_cancel = ctx.cancellation_token();
        // signal to stream to stop - due to cutover or fail in other streams
        let stream_stop = stop.child_token();

        let initial_lsn = slot.lsn();
        ctx.set_status(ReplicationShardStatus {
            lsn: initial_lsn,
            lag_bytes: None,
            missed_rows: MissedRows::default(),
        });

        let mut replication_run = Box::pin(replication_stream.run(slot, tables, &stream_stop));

        let mut report = safe_interval(Duration::from_secs(1));

        let result = loop {
            select! {
                _ = task_cancel.cancelled(), if !stream_stop.is_cancelled() => {
                    stream_stop.cancel();
                }
                result = &mut replication_run => {
                    break result;
                }
                _ = report.tick() => {
                    ctx.set_status(stream_status(&replication_stream, initial_lsn));
                }
            }
        };

        ctx.set_status(stream_status(&replication_stream, initial_lsn));

        result
    }
}

fn stream_status(replication: &ReplicationStream, fallback_lsn: Lsn) -> ReplicationShardStatus {
    let info = replication.progress();
    ReplicationShardStatus {
        lsn: info.applied_lsn.unwrap_or(fallback_lsn),
        lag_bytes: info.replication_lag,
        missed_rows: info.missed_rows,
    }
}

type ReplicationStreams = FuturesUnordered<AbortOnDropHandle<Result<(), Error>>>;

struct MaintenanceMode {
    stopped_traffic: bool,
}

impl MaintenanceMode {
    fn new() -> Self {
        Self {
            stopped_traffic: false,
        }
    }

    fn stop_traffic(&mut self) {
        maintenance_mode::start(None);
        self.stopped_traffic = true;
    }

    fn resume_traffic(self) {
        drop(self);
    }
}

impl Drop for MaintenanceMode {
    fn drop(&mut self) {
        if self.stopped_traffic {
            maintenance_mode::stop(None);
        }
    }
}

/// Cutover tokens of the replication tasks currently awaiting an operator
/// `CUTOVER`, keyed by the root task id they belong to. A cutover token is
/// *separate* from the task's `STOP_TASK` cancellation token — signalling it
/// means "cut over", not "abandon".
static CUTOVERS: LazyLock<DashMap<TaskId, CancellationToken>> = LazyLock::new(DashMap::new);

/// Guard held by a running replication task: removes its cutover
/// registration on drop. Awaiting [CutoverWaiter::requested]
/// resolves when an operator `CUTOVER` targets the task.
struct CutoverWaiter {
    root_id: TaskId,
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
        CUTOVERS.remove(&self.root_id);
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
        let waiter = ReplicationTask::register_cutover(TaskId::new(1));
        assert!(
            ReplicationTask::trigger_cutover(Some(TaskId::new(1))),
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
        let waiter = ReplicationTask::register_cutover(TaskId::new(7));

        assert!(
            !ReplicationTask::trigger_cutover(Some(TaskId::new(8))),
            "no task is registered under id 8"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(200), waiter.requested())
                .await
                .is_err(),
            "a cutover for a different id leaked to this task"
        );

        assert!(ReplicationTask::trigger_cutover(Some(TaskId::new(7))));
        tokio::time::timeout(Duration::from_secs(1), waiter.requested())
            .await
            .expect("targeted cutover was not delivered");
    }

    #[tokio::test]
    async fn cutover_without_id_targets_the_first_task() {
        let _guard = CUTOVER_TEST_LOCK.lock().await;
        // No id: the lowest-id (first) registered task is cut over, and only
        // it.
        let first = ReplicationTask::register_cutover(TaskId::new(3));
        let second = ReplicationTask::register_cutover(TaskId::new(9));

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
            let first = ReplicationTask::register_cutover(TaskId::new(1));
            assert!(ReplicationTask::trigger_cutover(Some(TaskId::new(1))));
            drop(first); // ends without ever awaiting `requested()`
        }

        let next = ReplicationTask::register_cutover(TaskId::new(2));
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
        assert!(!ReplicationTask::trigger_cutover(Some(TaskId::new(404))));
    }
}
