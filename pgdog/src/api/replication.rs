//! Logical-replication background task.

use std::pin::pin;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use dashmap::DashMap;
use futures::future::{FusedFuture, FutureExt};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use crate::api::Task;
use crate::api::schema_sync::{SchemaSyncBuilder, SchemaSyncPhase, SchemaSyncTask};
use crate::api::task::{TaskContext, TaskId};
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::publisher::cutover_policy::CutoverPolicy;
use crate::backend::replication::logical::publisher::replication_progress::ReplicationProgress;
use crate::backend::replication::logical::publisher::replication_stream::{
    DRAIN_TIMEOUT, ReplicationStream,
};
use crate::backend::replication::logical::publisher::{Lsn, Permanent, ReplicationSlot, Table};
use crate::backend::replication::logical::resharding_state::ReshardingState;
use crate::backend::schema::sync::SchemaSyncError;
use crate::backend::{
    databases::{cancel_all, cutover},
    maintenance_mode,
};
use crate::config::config;
use crate::tasks;
use crate::util::{safe_interval, safe_timeout};
use pgdog_config::resharding::PostDataValidationStage;
use pgdog_stats::{
    Databases, MissedRows, ReplicationClusterDefinition, ReplicationClusterStatus,
    ReplicationCutoverReason, ReplicationDefinition, ReplicationDirection,
    ReplicationShardDefinition, ReplicationShardStatus, ReplicationStatus, TaskDefinition,
};
use tracing::{info, warn};

#[derive(Debug, bon::Builder)]
pub(crate) struct ReplicationTask {
    pub(crate) state: ReshardingState,
    /// Cut over automatically once the destination has caught up, instead
    /// of waiting for an operator `CUTOVER`.
    #[builder(default)]
    pub(crate) auto_cutover: bool,
    pub(crate) schema_sync: SchemaSyncBuilder,
    pub(crate) validate: bool,
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
            databases: self.state.databases(),
            auto_cutover: self.auto_cutover,
        }
    }

    async fn run(self, ctx: TaskContext<Self>) -> Result<(), Error> {
        let Self {
            state,
            schema_sync,
            validate,
            auto_cutover,
        } = self;

        let mut replication = Replication::new(&ctx, state.clone(), schema_sync, validate);
        // run the replication until it's stopped - it can cutover multiple times but
        // it's stopped eventually only by the cancellation process.
        let result = replication.run(auto_cutover).await;
        replication.resume_traffic();

        let stopped_in_rollback_window = replication.cancelled()
            && replication.direction == ReplicationDirection::Reverse
            && matches!(result, Err(Error::ReplicationAborted));
        let completed = result.is_ok() || stopped_in_rollback_window;

        let cleanup = if completed {
            state.drop_slots().await
        } else {
            state.drop_slots_if_owned().await
        };

        if let Err(err) = cleanup {
            warn!("failed to clean up replication slots: {err}");
        }

        if stopped_in_rollback_window {
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
    state: ReshardingState,
    schema_sync: SchemaSyncBuilder,
    validation_stage: PostDataValidationStage,
    direction: ReplicationDirection,
    maintenance: MaintenanceMode,
}

impl<'a> Replication<'a> {
    fn new(
        ctx: &'a TaskContext<ReplicationTask>,
        state: ReshardingState,
        schema_sync: SchemaSyncBuilder,
        validate: bool,
    ) -> Self {
        let validation_stage = if validate {
            config().config.resharding.post_data_validation
        } else {
            PostDataValidationStage::Off
        };

        Self {
            ctx,
            state,
            schema_sync,
            validation_stage,
            direction: ReplicationDirection::Forward,
            maintenance: MaintenanceMode::new(),
        }
    }

    async fn run(&mut self, auto_cutover: bool) -> Result<(), Error> {
        info!(
            "[replication] starting {}, auto_cutover={auto_cutover}, post_data_validation={}",
            self.state.databases(),
            self.validation_stage
        );
        self.replicate_until_cutover(auto_cutover, PostDataValidationStage::DuringReplication)
            .await?;
        self.sync_schema(
            self.schema_sync
                .clone()
                .phase(SchemaSyncPhase::Cutover)
                .ignore_errors(true)
                .build(),
        )
        .await?;
        self.post_data_validation(PostDataValidationStage::BeforeCutover)
            .await?;

        loop {
            self.cutover().await?;
            self.flip_direction();
            self.replicate_until_cutover(false, PostDataValidationStage::AfterCutover)
                .await?;
            self.validation_stage = PostDataValidationStage::Off;
            self.sync_schema(
                SchemaSyncTask::builder()
                    .databases(self.state.databases())
                    .publication(self.state.publication.clone())
                    .phase(SchemaSyncPhase::Cutover)
                    .ignore_errors(true)
                    .build(),
            )
            .await?;
        }
    }

    /// Run the replication until we get the cutover signal and [`CutoverPolicy`]
    /// waited for the stop_traffic conditions. When the streams do not drain
    /// the source WAL in time, resume the traffic and start the replication again.
    async fn replicate_until_cutover(
        &mut self,
        auto_cutover: bool,
        stage: PostDataValidationStage,
    ) -> Result<(), Error> {
        let wait_for_validation = stage != PostDataValidationStage::AfterCutover;
        let ctx = self.ctx;
        let direction = self.direction;
        let task_cancel = ctx.cancellation_token();
        let mut cutover = (!auto_cutover).then(|| CutoverWaiter::register(ctx.root_id()));
        let mut validation = pin!(self.post_data_validation(stage).fuse());

        loop {
            let progress = ReplicationProgress::new(self.state.source.shards().len());
            let mut tables = self.state.tables();
            let mut cutover_reason = None;
            let (cluster, stop_cluster_replication) =
                ReplicationClusterTask::new(self.state.clone(), direction, progress.clone());

            info!("[replication] {direction} stream starting");
            ctx.set_status(match direction {
                ReplicationDirection::Forward => ReplicationStatus::Replicating,
                ReplicationDirection::Reverse => ReplicationStatus::ReverseReplicating,
            });
            let mut cluster_run = pin!(ctx.run(cluster).fuse());

            let result = async {
                let mut cutover_run = pin!(async {
                    if let Some(cutover) = cutover.as_ref() {
                        cutover.requested().await;
                    }
                    Self::prepare_cutover(ctx, &self.state, &mut self.maintenance, progress.clone())
                        .await
                });

                loop {
                    select! {
                        biased;
                        _ = task_cancel.cancelled() => {
                            info!("[replication] {direction} stream cancelled");
                            break Err(Error::ReplicationAborted);
                        },
                        result = &mut cluster_run => {
                            break result.and(Err(Error::ReplicationStreamStopped));
                        },
                        result = &mut validation => result?,
                        result = &mut cutover_run, if !wait_for_validation || validation.is_terminated() => {
                            break result.map(|reason| cutover_reason = Some(reason));
                        },
                    }
                }
            }
            .await;

            if result.is_err() {
                self.maintenance.resume_traffic();
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

            if result.is_ok() && matches!(drained, Err(Error::CatchUpTimeout)) {
                warn!(
                    "[replication] {direction} cutover aborted: {}, traffic resumed, restarting replication",
                    Error::CatchUpTimeout
                );
                self.maintenance.resume_traffic();
                // Changes up to the applied LSN are already on the destination.
                for (shard, tables) in &mut tables {
                    if let Some(applied) = progress.applied_lsn(*shard) {
                        for table in tables {
                            table.lsn = table.lsn.max(applied);
                        }
                    }
                }
                self.state.set_tables(tables);
                if let Some(cutover) = cutover.as_mut() {
                    cutover.rearm();
                }
                continue;
            }

            let result = result.and(drained);
            match &result {
                Ok(()) => info!("[replication] {direction} stream stopped"),
                Err(err) => warn!("[replication] {direction} stream failed: {err}"),
            }
            return result;
        }
    }

    fn post_data_validation(
        &self,
        stage: PostDataValidationStage,
    ) -> impl Future<Output = Result<(), Error>> + use<'a> {
        let ctx = self.ctx;
        let task = (self.validation_stage == stage).then(|| match stage {
            PostDataValidationStage::AfterCutover => self.reverse_validation(),
            _ => self
                .schema_sync
                .clone()
                .phase(SchemaSyncPhase::PostDataValidation)
                .build(),
        });
        async move {
            let Some(task) = task else {
                return Ok(());
            };
            match ctx.run(task).await {
                Ok(()) => {
                    info!("[replication] post-data validation finished at {stage}");
                    Ok(())
                }
                Err(SchemaSyncError::Aborted) if ctx.cancellation_token().is_cancelled() => {
                    Err(Error::ReplicationAborted)
                }
                Err(err) if stage == PostDataValidationStage::AfterCutover => {
                    warn!(
                        "[replication] post-data validation failed at {stage}, replication continues: {err}"
                    );
                    Ok(())
                }
                Err(err) => {
                    warn!("[replication] post-data validation failed at {stage}: {err}");
                    Err(err.into())
                }
            }
        }
    }

    fn reverse_validation(&self) -> SchemaSyncTask {
        let databases = self.state.databases();
        SchemaSyncTask::builder()
            .databases(Databases {
                source: databases.destination,
                destination: databases.source,
            })
            .publication(self.state.publication.clone())
            .phase(SchemaSyncPhase::PostDataValidation)
            .build()
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

    /// Wait for cutover initial conditions, stop the traffic
    /// and wait until the replication catch up with the source.
    /// Resumes the traffic on any error.
    async fn prepare_cutover(
        ctx: &TaskContext<ReplicationTask>,
        state: &ReshardingState,
        maintenance: &mut MaintenanceMode,
        progress: ReplicationProgress,
    ) -> Result<ReplicationCutoverReason, Error> {
        let cutover_policy = CutoverPolicy::new(config().as_ref().into(), progress);
        cutover_policy.wait_for_stop_threshold().await;
        ctx.set_status(ReplicationStatus::StoppingTraffic);
        maintenance.stop_traffic();
        let result = async {
            cancel_all(&state.source.identifier().database).await?;
            ctx.set_status(ReplicationStatus::WaitingForCatchUp);
            cutover_policy.wait_for_catchup().await
        }
        .await;
        if result.is_err() {
            maintenance.resume_traffic();
        }
        result
    }

    /// Execute the cutover: create reverse slots, update the config,
    /// refresh the state and resume traffic.
    async fn cutover(&mut self) -> Result<(), Error> {
        info!("Cutting over");
        self.ctx
            .set_status(ReplicationStatus::PreparingReverseReplication);

        // remove slots on the current source
        Box::pin(self.state.drop_slots()).await?;
        // create replication slots on the destination to allow rollback
        self.state
            .create_reverse_slots(&self.ctx.cancellation_token())
            .await?;
        self.ctx.set_status(match self.direction {
            ReplicationDirection::Forward => ReplicationStatus::CuttingOver,
            ReplicationDirection::Reverse => ReplicationStatus::RollingBack,
        });
        cutover(
            &self.state.source.identifier().database,
            &self.state.destination.identifier().database,
        )
        .await?;
        self.state.reload()?;
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
    /// With a cutover reason, every stream first applies the source WAL written up to now.
    pub(crate) fn stop(self, cutover_reason: Option<ReplicationCutoverReason>) {
        let _ = self.sender.send(cutover_reason);
    }
}

/// Task that runs the replication in one direction
/// from one source cluster to another.
#[derive(Debug)]
pub(crate) struct ReplicationClusterTask {
    state: ReshardingState,
    progress: ReplicationProgress,
    direction: ReplicationDirection,
    stop: tokio::sync::oneshot::Receiver<Option<ReplicationCutoverReason>>,
}

impl ReplicationClusterTask {
    pub(crate) fn new(
        state: ReshardingState,
        direction: ReplicationDirection,
        progress: ReplicationProgress,
    ) -> (Self, ReplicationClusterStop) {
        let (sender, stop) = tokio::sync::oneshot::channel();
        (
            Self {
                state,
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
            databases: self.state.databases(),
            direction: self.direction,
        }
    }

    async fn run(self, ctx: TaskContext<Self>) -> Result<(), Error> {
        let Self {
            state,
            progress,
            stop,
            direction,
        } = self;
        let task_cancel = ctx.cancellation_token();
        let mut streams = ReplicationStreams::new();
        let mut replication_streams = Vec::new();

        ctx.set_status(ReplicationClusterStatus::InitializingReplicationStreams);
        let init_result = Self::create_replication_shard_tasks(
            &ctx,
            &state,
            &progress,
            &mut replication_streams,
            &mut streams,
        )
        .await;

        let mut report = safe_interval(Duration::from_secs(1));
        let mut stop = stop;
        let mut drain = false;
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
                                info!("[replication] {direction} streams stopped for cutover ({reason}), draining up to the current source WAL");
                                drain = true;
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
        replication_streams
            .iter()
            .for_each(|stream| stream.stop(drain));
        let timeout = if drain {
            DRAIN_TIMEOUT + Self::stream_drain_timeout()
        } else {
            Self::stream_drain_timeout()
        };
        let drained = Self::drain_streams(&mut streams, timeout).await;
        result.and(drained)
    }
}

impl ReplicationClusterTask {
    /// Create [`ReplicationShardTask`] for every source shard in the cluster
    /// and track its status.
    async fn create_replication_shard_tasks(
        ctx: &TaskContext<Self>,
        state: &ReshardingState,
        progress: &ReplicationProgress,
        replication_streams: &mut Vec<Arc<ReplicationStream>>,
        streams: &mut ReplicationStreams,
    ) -> Result<(), Error> {
        state.prepare_replication(&ctx.cancellation_token()).await?;
        for source_shard in 0..state.source.shards().len() {
            let tables = state.pop_tables(source_shard)?;
            let slot = state.slot(source_shard)?;
            let updater = progress.updater_for_shard(source_shard);
            let replication_stream = Arc::new(ReplicationStream::new(
                &state.source,
                &state.destination,
                updater,
            ));
            replication_streams.push(Arc::clone(&replication_stream));
            let task = ReplicationShardTask::builder()
                .source_shard(source_shard)
                .slot(slot)
                .tables(tables)
                .replication_stream(replication_stream)
                .build();

            streams.push(AbortOnDropHandle::new(tasks::spawn(
                "replication stream",
                ctx.run(task),
            )));
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
    async fn drain_streams(
        streams: &mut ReplicationStreams,
        timeout: Duration,
    ) -> Result<(), Error> {
        let drained = safe_timeout(timeout, async {
            let mut result = Ok(());
            while let Some(child) = streams.next().await {
                result = result.and(child.map_err(Error::from).and_then(|result| result));
            }
            result
        })
        .await;

        match drained {
            Ok(result) => result,
            Err(_) => {
                streams.iter().for_each(AbortOnDropHandle::abort);
                while streams.next().await.is_some() {}
                Err(Error::DrainTimeout)
            }
        }
    }
}

/// Task for the replication stream executing on a single
/// source shard
#[derive(Debug, bon::Builder)]
pub(crate) struct ReplicationShardTask {
    pub(crate) slot: ReplicationSlot<Permanent>,
    pub(crate) source_shard: usize,
    pub(crate) tables: Vec<Table>,
    pub(crate) replication_stream: Arc<ReplicationStream>,
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
            source_shard,
        } = self;

        // task got cancelled
        let task_cancel = ctx.cancellation_token();

        let slot_name = slot.name().to_owned();
        let slot_addr = slot.addr().clone();

        slot.set_task_id(ctx.root_id());

        let mut stream = slot.get_existing().await?;

        let initial_lsn = stream.lsn();
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
        let mut replication_run = Box::pin(replication_stream.run(&mut stream, tables));

        let report_interval = Duration::from_secs(5);
        let mut report = safe_interval(report_interval);
        let mut logged_rows = 0u64;
        let mut logged_bytes = 0u64;

        let mut cancelled = false;
        let result = loop {
            select! {
                _ = task_cancel.cancelled(), if !cancelled => {
                    info!(shard = source_shard, "[replication] stream cancelled");
                    cancelled = true;
                    replication_stream.stop(false);
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
        drop(replication_run);
        drop(stream);
        let status = replication_stream.progress().snapshot(initial_lsn);
        let result = match result {
            Ok(()) => verify_confirmed_lsn(&slot, status.lsn).await,
            Err(err) => Err(err),
        };

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

        result
    }
}

async fn verify_confirmed_lsn(
    slot: &ReplicationSlot<Permanent>,
    expected: Lsn,
) -> Result<(), Error> {
    slot.reload().await?;
    let confirmed = slot.lsn();
    if confirmed < expected {
        return Err(Error::SlotLsnNotConfirmed {
            slot: slot.name().to_owned(),
            expected: expected.to_string(),
            confirmed: confirmed.to_string(),
        });
    }
    Ok(())
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

    /// Replace the latched token, so that the next cutover needs a new `CUTOVER`.
    fn rearm(&mut self) {
        self.token = CancellationToken::new();
        CUTOVERS.insert(self.root_id, self.token.clone());
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
