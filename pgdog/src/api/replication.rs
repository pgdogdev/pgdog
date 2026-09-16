use std::sync::LazyLock;
use std::time::Duration;

use dashmap::DashMap;
use futures::future::BoxFuture;
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
    Lsn, MissedRows, ReplicationDefinition, ReplicationStatus, ReplicationStreamDefinition,
    ReplicationStreamStatus, TaskDefinition,
};
use tracing::{info, warn};

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

#[derive(Debug, bon::Builder)]
pub(crate) struct ReplicationTask {
    pub(crate) orchestrator: Orchestrator,
    /// Cut over automatically once the destination has caught up, instead
    /// of waiting for an operator `CUTOVER`.
    #[builder(default)]
    pub(crate) auto_cutover: bool,
    /// Replication direction. `Reverse` marks the post-cutover stream that
    /// backs a rollback; it only affects reported status, not control flow.
    #[builder(default)]
    pub(crate) direction: Direction,
    pub(crate) schema_sync: SchemaSyncTask,
}

impl Task for ReplicationTask {
    type Status = ReplicationStatus;
    type Output = ();
    type Error = Error;

    fn cancel_timeout() -> Duration {
        Duration::from_secs(60)
    }

    fn definition(&self) -> impl Into<TaskDefinition> {
        ReplicationDefinition {
            databases: self.orchestrator.databases(),
            reverse: self.direction == Direction::Reverse,
            auto_cutover: self.auto_cutover,
        }
    }

    fn run(self, ctx: TaskContext<Self>) -> impl Future<Output = Result<(), Error>> + Send {
        let future: BoxFuture<'static, Result<(), Error>> = Box::pin(async move {
            let cancel = ctx.cancellation_token();
            let stop = CancellationToken::new();
            let _stop_guard = stop.drop_guard_ref();
            let guard = self.orchestrator.publication_guard();
            let mut streams = ReplicationStreams::new();
            let progress = ReplicationProgress::new(self.orchestrator.source.shards().len());

            ctx.set_status(ReplicationStatus::InitializingReplicationStreams);
            let result = async {
                let mut publisher = self.orchestrator.publisher().await;
                publisher
                    .prepare_replication(&self.orchestrator.source, &cancel)
                    .await?;
                for (source_shard, slot) in std::mem::take(&mut publisher.slots) {
                    let tables = publisher.tables.remove(&source_shard).unwrap_or_default();
                    let updater = progress.updater_for_shard(source_shard);
                    let replication_stream = ReplicationStream::new(
                        &self.orchestrator.source,
                        &self.orchestrator.destination,
                        updater,
                    );
                    let task = ReplicationStreamTask::builder()
                        .source_shard(source_shard)
                        .slot(slot)
                        .tables(tables)
                        .replication_stream(replication_stream)
                        .stop(stop.clone())
                        .build();
                    let child = ctx.run(task);
                    // Replicate in parallel.
                    streams.push(AbortOnDropHandle::new(tokio::spawn(child)));
                }
                drop(publisher);
                ctx.set_status(ReplicationStatus::Replicating);
                self.drive(&ctx, &cancel, &stop, &mut streams, progress)
                    .await
            }
            .await;

            stop.cancel();
            let drained = Self::drain(&mut streams).await;
            let cleanup = guard.cleanup().await;
            result.and(drained).and(cleanup)
        });
        future
    }
}

impl ReplicationTask {
    async fn drain(streams: &mut ReplicationStreams) -> Result<(), Error> {
        match safe_timeout(Self::cancel_timeout(), async {
            let mut result = Ok(());
            while let Some(child) = streams.next().await {
                result = result.and(child.map_err(Error::from).and_then(|result| result));
            }
            result
        })
        .await
        {
            Ok(result) => result,
            Err(_) => {
                streams.clear();
                Err(Error::ReplicationTimeout)
            }
        }
    }

    async fn drive(
        self,
        ctx: &TaskContext<Self>,
        cancel: &CancellationToken,
        stop: &CancellationToken,
        streams: &mut ReplicationStreams,
        progress: ReplicationProgress,
    ) -> Result<(), Error> {
        if self.auto_cutover {
            return self
                .perform_cutover(ctx, cancel, stop, streams, progress)
                .await;
        }

        let cutover = Self::register_cutover(ctx.root_id());
        loop {
            select! {
                biased;
                _ = cancel.cancelled() => {
                    ctx.set_status(ReplicationStatus::Stopping);
                    return Ok(());
                }
                result = streams.next() => {
                    match result {
                        Some(result) => result??,
                        None => return Ok(()),
                    }
                }
                _ = cutover.requested() => {
                    return self.perform_cutover(ctx, cancel, stop, streams, progress).await;
                }
            }
        }
    }

    async fn perform_cutover(
        mut self,
        ctx: &TaskContext<Self>,
        cancel: &CancellationToken,
        stop: &CancellationToken,
        streams: &mut ReplicationStreams,
        progress: ReplicationProgress,
    ) -> Result<(), Error> {
        let _resume = ResumeTraffic;
        ctx.set_status(match self.direction {
            Direction::Forward => ReplicationStatus::CuttingOver,
            Direction::Reverse => ReplicationStatus::RollingBack,
        });

        async {
            let cutover_policy = CutoverPolicy::new(config().as_ref().into(), progress);
            {
                let thresholds = async {
                    cutover_policy.wait_for_stop_threshold().await?;
                    maintenance_mode::start(None);
                    cancel_all(&self.orchestrator.source.identifier().database).await?;
                    cutover_policy.wait_for_cutover().await
                };
                tokio::pin!(thresholds);
                loop {
                    select! {
                        biased;
                        _ = cancel.cancelled() => {
                            ctx.set_status(ReplicationStatus::Stopping);
                            return Ok(());
                        }
                        result = streams.next() => {
                            match result {
                                Some(result) => result??,
                                None => return Ok(()),
                            }
                        }
                        result = &mut thresholds => {
                            result?;
                            break;
                        }
                    }
                }
            }

            stop.cancel();
            Self::drain(streams).await?;
            ctx.run(self.schema_sync).await?;
            // Traffic is about to go to the new cluster.
            // If this fails, we'll resume traffic to the old cluster instead
            // and the whole thing needs to be done from scratch.
            cutover(
                &self.orchestrator.source.identifier().database,
                &self.orchestrator.destination.identifier().database,
            )
            .await?;

            // Source is now destination and vice versa; reload cluster refs and
            // create a fresh publisher for reverse replication.
            self.orchestrator.refresh()?;
            self.orchestrator.refresh_publisher();
            info!("[cutover] setting up reverse replication");

            // Create reverse replication in case we need to rollback.
            let guard = self.orchestrator.publication_guard();
            let reverse_slots = self
                .orchestrator
                .publisher()
                .await
                .create_slots(&self.orchestrator.source, &CancellationToken::new())
                .await;
            if let Err(err) = reverse_slots {
                if let Err(cleanup) = guard.cleanup().await {
                    warn!("failed to clean up reverse replication slots: {cleanup}");
                }
                return Err(err);
            }

            let schema_sync = SchemaSyncTask::builder()
                .databases(self.orchestrator.databases())
                .publication(self.orchestrator.publication.clone())
                .phase(SchemaSyncPhase::Cutover)
                .ignore_errors(true)
                .build();
            crate::api::run_task(
                Self::builder()
                    .orchestrator(self.orchestrator)
                    .direction(Direction::Reverse)
                    .schema_sync(schema_sync)
                    .build(),
            );

            // Slot is established and capturing — now safe to resume traffic.
            info!("[cutover] complete, resuming traffic");
            Ok(())
        }
        .await
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

#[derive(Debug, bon::Builder)]
pub(crate) struct ReplicationStreamTask {
    pub(crate) slot: ReplicationSlot,
    pub(crate) source_shard: usize,
    pub(crate) tables: Vec<Table>,
    pub(crate) replication_stream: ReplicationStream,
    pub(crate) stop: CancellationToken,
}

impl Task for ReplicationStreamTask {
    type Status = ReplicationStreamStatus;
    type Output = ();
    type Error = Error;

    fn cancel_timeout() -> Duration {
        Duration::from_secs(60)
    }

    fn definition(&self) -> impl Into<TaskDefinition> {
        ReplicationStreamDefinition {
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

        let cancel_token = ctx.cancellation_token();
        let replication_cancel = stop.child_token();

        let initial_lsn = slot.lsn();
        ctx.set_status(ReplicationStreamStatus {
            lsn: initial_lsn,
            lag_bytes: None,
            missed_rows: MissedRows::default(),
        });

        let mut replication_run =
            Box::pin(replication_stream.run(slot, tables, &replication_cancel));

        let mut report = safe_interval(Duration::from_secs(1));

        let result = loop {
            select! {
                _ = cancel_token.cancelled(), if !replication_cancel.is_cancelled() => {
                    replication_cancel.cancel();
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

fn stream_status(replication: &ReplicationStream, fallback_lsn: Lsn) -> ReplicationStreamStatus {
    let info = replication.progress();
    ReplicationStreamStatus {
        lsn: info.applied_lsn.unwrap_or(fallback_lsn),
        lag_bytes: info.replication_lag,
        missed_rows: info.missed_rows,
    }
}

type ReplicationStreams = FuturesUnordered<AbortOnDropHandle<Result<(), Error>>>;

struct ResumeTraffic;

impl Drop for ResumeTraffic {
    fn drop(&mut self) {
        maintenance_mode::stop(None);
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
