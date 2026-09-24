//! Logical-replication background task.

use std::pin::pin;
use std::sync::LazyLock;
use std::time::Duration;

use dashmap::DashMap;
use futures::future::{FusedFuture, FutureExt};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::select;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::api::Task;
use crate::api::schema_sync::{SchemaSyncPhase, SchemaSyncTask};
use crate::api::task::{TaskContext, TaskId};
use crate::backend::replication::ee::{OrchestratorState, orchestrator_state};
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
use crate::tasks;
use crate::util::{safe_interval, safe_timeout};
use pgdog_stats::{
    MissedRows, ReplicationClusterDefinition, ReplicationClusterStatus, ReplicationCutoverReason,
    ReplicationDefinition, ReplicationDirection, ReplicationShardDefinition,
    ReplicationShardStatus, ReplicationStatus, TaskDefinition,
};
use tracing::{info, warn};

#[derive(Debug, bon::Builder)]
pub(crate) struct ReplicationTask {
    pub(crate) orchestrator: Orchestrator,
    /// Cut over automatically once the destination has caught up, instead
    /// of waiting for an operator `CUTOVER`.
    #[builder(default)]
    pub(crate) auto_cutover: bool,
    pub(crate) schema_sync: SchemaSyncTask,
}

/// Executes the whole replication process. It runs a replication until a cutover,
/// cuts over, then runs the opposite replication so a rollback stays possible. Each
/// cutover flips the direction, so the task never finishes on its own.
///
/// A `STOP_TASK` during a reverse phase returns `Ok(())`, so the task reports
/// as finished: the migration is complete and the operator ended the rollback
/// window. The same signal during a forward phase returns
/// [`Error::ReplicationAborted`], which reports as cancelled.
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
            orchestrator,
            schema_sync,
            auto_cutover,
        } = self;

        let slots = orchestrator.publication_guard();
        let mut replication = Replication::new(&ctx, orchestrator);
        let result = replication.run(schema_sync, auto_cutover).await;
        replication.resume_traffic();
        if let Err(err) = slots.cleanup().await {
            warn!("failed to clean up replication slots: {err}");
        }

        if replication.cancelled()
            && replication.direction == ReplicationDirection::Reverse
            && matches!(result, Err(Error::ReplicationAborted))
        {
            info!("[replication] stopped in the rollback window, migration complete");
            return Ok(());
        }

        match &result {
            Ok(()) => info!("[replication] finished"),
            Err(err) if replication.cancelled() => info!("[replication] cancelled: {err}"),
            Err(err) => warn!("[replication] failed: {err}"),
        }

        result
    }
}

impl ReplicationTask {
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
}

/// Struct to hold the replication state from the [`ReplicationTask`]
struct Replication<'a> {
    ctx: &'a TaskContext<ReplicationTask>,
    orchestrator: Orchestrator,
    direction: ReplicationDirection,
    maintenance: MaintenanceMode,
}

impl<'a> Replication<'a> {
    fn new(ctx: &'a TaskContext<ReplicationTask>, orchestrator: Orchestrator) -> Self {
        Self {
            ctx,
            orchestrator,
            direction: ReplicationDirection::Forward,
            maintenance: MaintenanceMode::new(),
        }
    }

    async fn run(&mut self, schema_sync: SchemaSyncTask, auto_cutover: bool) -> Result<(), Error> {
        info!(
            "[replication] starting {}, auto_cutover={auto_cutover}",
            self.orchestrator.databases()
        );
        self.replicate_until_cutover(auto_cutover).await?;
        self.sync_schema(schema_sync).await?;

        loop {
            self.cutover().await?;
            self.flip_direction();
            self.replicate_until_cutover(false).await?;
            self.sync_schema(
                // new schema sync tasks with updated orchestrator
                SchemaSyncTask::builder()
                    .databases(self.orchestrator.databases())
                    .publication(self.orchestrator.publication.clone())
                    .phase(SchemaSyncPhase::Cutover)
                    .ignore_errors(true)
                    .build(),
            )
            .await?;
        }
    }

    fn resume_traffic(&mut self) {
        self.maintenance.resume_traffic();
    }

    fn cancelled(&self) -> bool {
        self.ctx.cancellation_token().is_cancelled()
    }

    async fn sync_schema(&self, schema_sync: SchemaSyncTask) -> Result<(), Error> {
        info!("Run schema sync in {} direction", self.direction);
        self.ctx.set_status(ReplicationStatus::SyncingSchema);
        self.ctx.run(schema_sync).await?;
        Ok(())
    }

    fn flip_direction(&mut self) {
        self.direction = match self.direction {
            ReplicationDirection::Reverse => ReplicationDirection::Forward,
            ReplicationDirection::Forward => ReplicationDirection::Reverse,
        };
    }

    /// Run the replication until we get the cutover signal and [`CutoverPolicy`]
    /// waited for the stop_traffic conditions
    async fn replicate_until_cutover(&mut self, auto_cutover: bool) -> Result<(), Error> {
        let ctx = self.ctx;
        let task_cancel = ctx.cancellation_token();
        let cutover = (!auto_cutover).then(|| CutoverWaiter::register(ctx.root_id()));
        let progress = ReplicationProgress::new(self.orchestrator.source.shards().len());
        let mut cutover_reason = None;
        let (cluster, stop_cluster_replication) = ReplicationClusterTask::new(
            self.orchestrator.clone(),
            self.direction,
            progress.clone(),
        );

        info!("[replication] {} stream starting", self.direction);
        orchestrator_state(OrchestratorState::Replication);
        ctx.set_status(match self.direction {
            ReplicationDirection::Forward => ReplicationStatus::Replicating,
            ReplicationDirection::Reverse => ReplicationStatus::ReverseReplicating,
        });
        let mut cluster_run = pin!(ctx.run(cluster).fuse());

        let result = select! {
            biased;
            _ = task_cancel.cancelled() => {
                info!("[replication] {} stream cancelled", self.direction);
                Err(Error::ReplicationAborted)
            },
            result = &mut cluster_run => result.and(Err(Error::ReplicationStreamStopped)),
            result = async {
                if let Some(cutover) = cutover.as_ref() {
                    cutover.requested().await;
                }
                self.prepare_cutover(progress).await
            } => result.map(|reason| cutover_reason = Some(reason)),
        };

        if result.is_err() {
            self.resume_traffic();
        }

        // stop the cluster replication and wait until it gracefully finishes,
        // we should stop it despite if we succeed or not at this moment
        stop_cluster_replication.stop(cutover_reason);
        let drained = if cluster_run.is_terminated() {
            Ok(())
        } else {
            safe_timeout(ReplicationClusterTask::drain_timeout(), &mut cluster_run)
                .await
                .unwrap_or(Err(Error::DrainTimeout))
        };
        let result = result.and(drained);
        match &result {
            Ok(()) => info!("[replication] {} stream stopped", self.direction),
            Err(err) => warn!("[replication] {} stream failed: {err}", self.direction),
        }
        result
    }

    /// Wait for cutover initial conditions, stop the traffic
    /// and wait until the replication catch up with the source
    async fn prepare_cutover(
        &mut self,
        progress: ReplicationProgress,
    ) -> Result<ReplicationCutoverReason, Error> {
        let cutover_policy = CutoverPolicy::new(config().as_ref().into(), progress);
        cutover_policy.wait_for_stop_threshold().await;
        self.ctx.set_status(ReplicationStatus::StoppingTraffic);
        self.maintenance.stop_traffic();
        let result = async {
            cancel_all(&self.orchestrator.source.identifier().database).await?;
            self.ctx.set_status(ReplicationStatus::WaitingForCatchUp);
            cutover_policy.wait_for_catchup().await
        }
        .await;
        if result.is_err() {
            // in case of errors, after stop_traffic, resume it immediately
            self.maintenance.resume_traffic();
        }
        result
    }

    /// Execute the cutover: create reverse slots, update the config,
    /// refresh the orchestrator and resume traffic.
    async fn cutover(&mut self) -> Result<(), Error> {
        info!("Cutting over");
        self.ctx
            .set_status(ReplicationStatus::PreparingReverseReplication);
        self.orchestrator
            .publisher()
            .await
            .create_slots(
                &self.orchestrator.destination,
                &self.ctx.cancellation_token(),
            )
            .await?;
        self.ctx.set_status(match self.direction {
            ReplicationDirection::Forward => ReplicationStatus::CuttingOver,
            ReplicationDirection::Reverse => ReplicationStatus::RollingBack,
        });
        cutover(
            &self.orchestrator.source.identifier().database,
            &self.orchestrator.destination.identifier().database,
        )
        .await?;
        self.orchestrator.refresh()?;
        self.maintenance.resume_traffic();
        Ok(())
    }
}

/// Handle that stops one replication cluster, with the cutover reason when
/// the parent task stopped it to cut traffic over.
#[derive(Debug)]
pub(crate) struct ReplicationClusterStop {
    sender: tokio::sync::oneshot::Sender<Option<ReplicationCutoverReason>>,
}

impl ReplicationClusterStop {
    pub(crate) fn stop(self, cutover_reason: Option<ReplicationCutoverReason>) {
        let _ = self.sender.send(cutover_reason);
    }
}

/// Task that runs the replication in one direction
/// from one source cluster to another.
#[derive(Debug)]
pub(crate) struct ReplicationClusterTask {
    orchestrator: Orchestrator,
    progress: ReplicationProgress,
    direction: ReplicationDirection,
    stop: tokio::sync::oneshot::Receiver<Option<ReplicationCutoverReason>>,
}

impl ReplicationClusterTask {
    pub(crate) fn new(
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
        Duration::from_secs(120)
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
            direction,
        } = self;
        let task_cancel = ctx.cancellation_token();
        let mut streams = ReplicationStreams::new();
        let streams_stop = CancellationToken::new();
        let _streams_stop_on_drop = streams_stop.clone().drop_guard();

        ctx.set_status(ReplicationClusterStatus::InitializingReplicationStreams);
        let init_result = Self::create_replication_shard_tasks(
            &ctx,
            &orchestrator,
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
                    direction,
                    progress: progress.snapshot(),
                });
                select! {
                    biased;
                    _ = task_cancel.cancelled() => {
                        info!("[replication] {direction} streams cancelled, draining");
                        return Ok(());
                    }
                    stopped = &mut stop => {
                        match stopped {
                            Ok(Some(reason)) => {
                                info!("[replication] {direction} streams stopped for cutover ({reason}), draining");
                                ctx.set_status(ReplicationClusterStatus::StoppedForCutover { reason });
                            }
                            _ => info!("[replication] {direction} streams stopped, draining"),
                        }
                        return Ok(());
                    }
                    // if any of streams exit early, stop the process
                    result = streams.next() => {
                        if let Some(child) = result {
                            child??;
                        }
                        // return an error, since it should stop by the signal
                        // not by itself
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
        result.and(drained)
    }
}

impl ReplicationClusterTask {
    /// Create [`ReplicationShardTask`] for every source shard in the cluster
    /// and track its status.
    async fn create_replication_shard_tasks(
        ctx: &TaskContext<Self>,
        orchestrator: &Orchestrator,
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
            let slot = SlotGuard::new(publisher.pop_slot(source_shard)?);
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

            streams.push(tasks::spawn("replication stream", ctx.run(task)));
        }

        Ok(())
    }

    fn drain_timeout() -> Duration {
        Duration::from_secs(300)
    }

    fn stream_drain_timeout() -> Duration {
        Duration::from_secs(120)
    }

    /// Drain all the streams - make sure they are drained and generated no errors
    async fn drain_streams(streams: &mut ReplicationStreams) -> Result<(), Error> {
        safe_timeout(Self::stream_drain_timeout(), async {
            let mut result = Ok(());
            while let Some(child) = streams.next().await {
                result = result.and(child.map_err(Error::from).and_then(|result| result));
            }
            result
        })
        .await
        .unwrap_or_else(|_| {
            streams.iter().for_each(JoinHandle::abort);
            Err(Error::DrainTimeout)
        })
    }
}

/// Task for the replication stream executing on a single
/// source shard
#[derive(Debug, bon::Builder)]
pub(crate) struct ReplicationShardTask {
    pub(crate) slot: SlotGuard,
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
        let slot = self.slot.get();
        ReplicationShardDefinition {
            slot: slot.name().to_owned(),
            host: slot.addr().host.clone(),
            port: slot.addr().port,
            database_name: slot.addr().database_name.clone(),
            source_shard: self.source_shard,
        }
    }

    async fn run(self, ctx: TaskContext<Self>) -> Result<(), Error> {
        let Self {
            mut slot,
            tables,
            replication_stream,
            stop,
            source_shard,
        } = self;

        // task got cancelled
        let task_cancel = ctx.cancellation_token();
        // signal to stream to stop - due to cutover or fail in other streams
        let stream_stop = stop.child_token();

        slot.slot().set_task_id(ctx.id());
        let slot_name = slot.get().name().to_owned();
        let slot_addr = slot.get().addr().clone();
        let initial_lsn = slot.get().lsn();
        ctx.set_status(ReplicationShardStatus {
            lsn: initial_lsn,
            lag_bytes: None,
            missed_rows: MissedRows::default(),
            rows: 0,
            bytes: 0,
            rows_per_sec: None,
            bytes_per_sec: None,
        });

        info!(
            shard = source_shard,
            "[replication] stream starting at {initial_lsn}"
        );
        let mut replication_run =
            Box::pin(replication_stream.run(slot.slot(), tables, &stream_stop));

        let report_interval = Duration::from_secs(5);
        let mut report = safe_interval(report_interval);
        let mut logged_rows = 0u64;
        let mut logged_bytes = 0u64;

        let result = loop {
            select! {
                _ = task_cancel.cancelled(), if !stream_stop.is_cancelled() => {
                    info!(shard = source_shard, "[replication] stream cancelled");
                    stream_stop.cancel();
                }
                result = &mut replication_run => {
                    break result;
                }
                _ = report.tick() => {
                    let progress = replication_stream.progress();
                    let status = progress.snapshot(initial_lsn);
                    let window = report_interval.as_secs_f64();
                    info!(
                        shard = source_shard,
                        addr = %slot_addr,
                        slot = slot_name,
                        "[replication] origin LSN at {}, speed over the last {}s: {:.0} rows/sec, {:.3} MB/sec",
                        progress.origin_lsn,
                        report_interval.as_secs(),
                        (status.rows - logged_rows) as f64 / window,
                        (status.bytes - logged_bytes) as f64 / window / 1024.0 / 1024.0,
                    );
                    logged_rows = status.rows;
                    logged_bytes = status.bytes;
                    ctx.set_status(status);
                }
            }
        };

        let status = replication_stream.progress().snapshot(initial_lsn);
        match &result {
            Ok(()) => info!(
                shard = source_shard,
                "[replication] stream stopped, {status}"
            ),
            Err(err) => warn!(
                shard = source_shard,
                "[replication] stream failed: {err}, {status}"
            ),
        }
        ctx.set_status(status);
        drop(replication_run);

        let dropped = Box::pin(slot.drop_slot()).await;
        if let Err(err) = &dropped {
            warn!("failed to drop replication slot {slot_name}: {err}");
        }

        result.and(dropped)
    }
}

type ReplicationStreams = FuturesUnordered<JoinHandle<Result<(), Error>>>;

/// Owns the replication slot of one stream.
///
/// Dropping this guard with the slot still inside schedules the drop on a
/// detached task, so an aborted stream cannot leak a permanent slot.
#[derive(Debug)]
pub(crate) struct SlotGuard {
    slot: Option<ReplicationSlot>,
}

impl SlotGuard {
    fn new(slot: ReplicationSlot) -> Self {
        Self { slot: Some(slot) }
    }

    fn slot(&mut self) -> &mut ReplicationSlot {
        self.slot.as_mut().expect("slot guard owns the slot")
    }

    fn get(&self) -> &ReplicationSlot {
        self.slot.as_ref().expect("slot guard owns the slot")
    }

    fn drop_timeout() -> Duration {
        Duration::from_secs(30)
    }

    async fn drop_slot(mut self) -> Result<(), Error> {
        let mut slot = self.slot.take().expect("slot guard owns the slot");
        let name = slot.name().to_owned();

        safe_timeout(Self::drop_timeout(), slot.drop_slot())
            .await
            .unwrap_or(Err(Error::SlotDropTimeout(name)))
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        let Some(mut slot) = self.slot.take() else {
            return;
        };
        let name = slot.name().to_owned();
        tasks::spawn("replication slot cleanup", async move {
            let dropped = safe_timeout(Self::drop_timeout(), slot.drop_slot())
                .await
                .unwrap_or(Err(Error::SlotDropTimeout(name.clone())));

            if let Err(err) = dropped {
                warn!("failed to drop replication slot {name} of an aborted stream: {err}");
            }
        });
    }
}

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

    fn resume_traffic(&mut self) {
        if self.stopped_traffic {
            maintenance_mode::stop(None);
            self.stopped_traffic = false;
        }
    }
}

impl Drop for MaintenanceMode {
    fn drop(&mut self) {
        self.resume_traffic();
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
    /// Register a task (by its `root_id`) to receive operator cutovers for
    /// as long as the returned guard is held.
    fn register(root_id: TaskId) -> Self {
        let token = CancellationToken::new();
        CUTOVERS.insert(root_id, token.clone());
        Self { root_id, token }
    }

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
        let waiter = CutoverWaiter::register(TaskId::new(1));
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
        let waiter = CutoverWaiter::register(TaskId::new(7));

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
        let first = CutoverWaiter::register(TaskId::new(3));
        let second = CutoverWaiter::register(TaskId::new(9));

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
            let first = CutoverWaiter::register(TaskId::new(1));
            assert!(ReplicationTask::trigger_cutover(Some(TaskId::new(1))));
            drop(first); // ends without ever awaiting `requested()`
        }

        let next = CutoverWaiter::register(TaskId::new(2));
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

    static MAINTENANCE_TEST_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

    fn traffic_stopped() -> bool {
        maintenance_mode::waiter("pgdog").is_some()
    }

    #[tokio::test]
    async fn maintenance_guard_stops_and_resumes_traffic() {
        let _guard = MAINTENANCE_TEST_LOCK.lock().await;
        let mut maintenance = MaintenanceMode::new();
        assert!(!traffic_stopped());

        maintenance.stop_traffic();
        assert!(traffic_stopped());

        maintenance.resume_traffic();
        assert!(!traffic_stopped());

        maintenance.resume_traffic();
        assert!(!traffic_stopped());
    }

    #[tokio::test]
    async fn dropping_the_maintenance_guard_resumes_traffic() {
        let _guard = MAINTENANCE_TEST_LOCK.lock().await;
        let mut maintenance = MaintenanceMode::new();
        maintenance.stop_traffic();
        assert!(traffic_stopped());

        drop(maintenance);
        assert!(!traffic_stopped());
    }
}
