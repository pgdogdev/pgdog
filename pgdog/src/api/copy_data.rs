use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::api::Task;
use crate::api::task::TaskContext;
use crate::backend::Cluster;
use crate::backend::pool::Address;
use crate::backend::replication::data_sync::{
    CopyProgress, DataSync, estimate_table, validate_destination_has_rows,
    validate_replica_identity,
};
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::resharding_state::ReshardingState;
use crate::backend::replication::publisher::{Table, resolve_resharding_replicas};
use crate::tasks;
use crate::util::safe_sleep;
use crate::util::stats::average_rate;
use crate::util::sync::WorkerPool;
use futures::prelude::stream::{FuturesUnordered, StreamExt};
use pgdog_config::CopyFormat;
use pgdog_stats::{
    CopyDataDefinition, CopyDataStage, CopyDataStatus, TableCopyDefinition, TableCopyStage,
    TableCopyStatus, TaskDefinition,
};
use tokio_util::task::AbortOnDropHandle;
use tracing::{info, warn};

const STATUS_REPORT_INTERVAL: Duration = Duration::from_secs(1);
const LOG_REPORT_INTERVAL: Duration = Duration::from_secs(5);

/// Bulk-copy table data from a source database to a target.
///
/// # Invariants
///
/// This task creates replication slots before copying tables.
/// These slots keep the changes made while the copy runs.
/// Each table copy uses its own snapshot, so the copied tables can be
/// inconsistent with each other until
/// [`SynchronizeTablesTask`](crate::api::synchronize_tables::SynchronizeTablesTask) runs.
#[derive(Debug, bon::Builder)]
pub(crate) struct CopyDataTask {
    pub(crate) state: ReshardingState,
    pub(crate) format: CopyFormat,
}

type TableTaskHandles = FuturesUnordered<AbortOnDropHandle<Result<(usize, Table), Error>>>;

impl Task for CopyDataTask {
    type Status = CopyDataStatus;
    type Output = ();
    type Error = Error;

    fn definition(&self) -> impl Into<TaskDefinition> {
        CopyDataDefinition {
            databases: self.state.databases(),
            format: self.format,
        }
    }

    async fn run(self, ctx: TaskContext<Self>) -> Result<(), Error> {
        let source = &self.state.source;
        let dest = &self.state.destination;
        let state = &self.state;
        let cancel = ctx.cancellation_token();

        ctx.set_status(CopyDataStage::LoadingTableMetadata.into());
        state.sync_tables().await?;

        ctx.set_status(CopyDataStage::ValidatingTables.into());
        validate_replica_identity(&state.tables())?;

        ctx.set_status(CopyDataStage::CreatingSlots.into());
        state.create_slots(&cancel).await?;

        let mut tables = state.tables();
        ctx.set_status(CopyDataStatus {
            stage: CopyDataStage::CopyingTables,
            tables_per_shard: Some(
                source
                    .shards()
                    .iter()
                    .map(|shard| tables.get(&shard.number()).map_or(0, |t| t.len() as u64))
                    .collect(),
            ),
        });

        let handles = TableTaskHandles::new();

        for shard in source.shards().iter() {
            let shard_tables = tables.remove(&shard.number()).unwrap_or_default();

            info!(
                "table sync starting for {} tables, shard={}",
                shard_tables.len(),
                shard.number()
            );

            let resharding_replicas = resolve_resharding_replicas(shard);

            let pool = Arc::new(WorkerPool::new(
                resharding_replicas,
                NonZeroUsize::new(dest.resharding_parallel_copies())
                    .unwrap_or(NonZeroUsize::new(1).unwrap()),
            )?);

            for table in shard_tables {
                let pool = Arc::clone(&pool);
                let source = source.clone();
                let dest = dest.clone();
                let ctx = ctx.clone();
                let shard_number = shard.number();
                let format = self.format;
                handles.push(AbortOnDropHandle::new(tasks::spawn(
                    "tables copy",
                    async move {
                        let table_sync_task = TableDataSyncTask::builder()
                            .pool(pool)
                            .table(table)
                            .source(source)
                            .dest(dest)
                            .format(format)
                            .source_shard(shard_number)
                            .build();

                        let table = ctx.run(table_sync_task).await?;

                        Ok((shard_number, table))
                    },
                )));
            }
        }

        let result = Self::drain_handles(&ctx, handles).await?;

        state.set_tables(result);

        Ok(())
    }
}

impl CopyDataTask {
    async fn drain_handles(
        ctx: &TaskContext<CopyDataTask>,
        mut handles: TableTaskHandles,
    ) -> Result<HashMap<usize, Vec<Table>>, Error> {
        let mut result = Ok(());
        let mut result_map: HashMap<usize, Vec<Table>> = HashMap::new();

        while let Some(res) = handles.next().await {
            if let Ok(Ok((shard, table))) = res {
                result_map.entry(shard).or_default().push(table);
                continue;
            }

            // cancel all other subtasks if we got an error
            ctx.subtasks_cancellation_token().cancel();
            result = result.and(res.map_err(Error::from).and_then(|r| r.map(|_| ())));
        }

        result.map(|()| result_map)
    }
}

#[derive(Debug, bon::Builder)]
pub(crate) struct TableDataSyncTask {
    pub(crate) pool: Arc<WorkerPool<Address>>,
    pub(crate) table: Table,
    pub(crate) source: Cluster,
    pub(crate) dest: Cluster,
    pub(crate) format: CopyFormat,
    pub(crate) source_shard: usize,
}

impl Task for TableDataSyncTask {
    type Status = TableCopyStatus;
    type Output = Table;

    type Error = Error;

    fn definition(&self) -> impl Into<TaskDefinition> {
        TableCopyDefinition {
            schema: self.table.table.schema.to_string(),
            table: self.table.table.name.to_string(),
            source_shard: self.source_shard,
        }
    }

    async fn run(self, ctx: TaskContext<Self>) -> Result<Self::Output, Self::Error> {
        let max_retries = self.dest.resharding_copy_retry_max_attempts();
        let base_delay = *self.dest.resharding_copy_retry_min_delay();
        let mut attempt = 0usize;
        let sync = DataSync {
            source: &self.source,
            dest: &self.dest,
            format: self.format,
            task_id: ctx.id(),
        };
        let cancel = ctx.cancellation_token();
        let mut last_error = None;

        loop {
            let mut status = TableCopyStatus {
                stage: TableCopyStage::WaitingForCopyHandler,
                attempt: attempt + 1,
                estimated_rows: None,
                estimated_bytes: None,
                rows_per_sec: None,
                bytes_per_sec: None,
                last_error: last_error.clone(),
            };
            ctx.set_status(status.clone());

            let Some(addr) = cancel.run_until_cancelled(self.pool.acquire()).await else {
                return Err(Error::DataSyncAborted);
            };
            let addr = addr?;

            status.stage = TableCopyStage::Estimation;
            ctx.set_status(status.clone());

            let estimate = estimate_table(&self.table, &addr)
                .await
                .inspect_err(|error| {
                    warn!(
                        "could not estimate size of \"{}\".\"{}\": {error}",
                        self.table.table.schema, self.table.table.name
                    )
                })
                .ok();
            status.estimated_rows = estimate.and_then(|e| e.rows);
            status.estimated_bytes = estimate.map(|e| e.bytes);
            status.stage = TableCopyStage::InProgress { rows: 0, bytes: 0 };
            ctx.set_status(status.clone());

            let mut reported = CopyProgress::default();
            let mut logged = CopyProgress::default();
            let copy_started = Instant::now();
            let mut last_report = Instant::now();
            let mut last_log = Instant::now();
            let result = Box::pin(sync.copy_table(&self.table, &addr, &cancel, |copied| {
                reported = copied;
                if last_report.elapsed() >= STATUS_REPORT_INTERVAL {
                    status.stage = TableCopyStage::InProgress {
                        rows: copied.rows,
                        bytes: copied.bytes,
                    };
                    status.rows_per_sec = average_rate(copied.rows, copy_started);
                    status.bytes_per_sec = average_rate(copied.bytes, copy_started);
                    ctx.set_status(status.clone());
                    last_report = Instant::now();
                }
                if last_log.elapsed() >= LOG_REPORT_INTERVAL {
                    let window = last_log.elapsed().as_secs_f64();
                    info!(
                        "synced {:.3} MB for table \"{}\".\"{}\" [{:.3} MB/sec]",
                        copied.bytes as f64 / 1024.0 / 1024.0,
                        self.table.table.schema,
                        self.table.table.name,
                        (copied.bytes - logged.bytes) as f64 / window / 1024.0 / 1024.0,
                    );
                    logged = copied;
                    last_log = Instant::now();
                }
            }))
            .await;
            drop(addr);
            status.stage = TableCopyStage::InProgress {
                rows: reported.rows,
                bytes: reported.bytes,
            };
            status.rows_per_sec = average_rate(reported.rows, copy_started);
            status.bytes_per_sec = average_rate(reported.bytes, copy_started);
            ctx.set_status(status.clone());

            match result {
                Ok(table) => return Ok(table),
                Err(err) if !err.is_retryable() || attempt >= max_retries => {
                    // Terminal failure: warn if rows remain so the operator can truncate.
                    let _ = self.can_retry().await;
                    return Err(err);
                }
                Err(err) => {
                    let backoff = base_delay * 2u32.pow(attempt.min(5) as u32);
                    attempt += 1;

                    warn!(
                        "data sync for \"{}\".\"{}\" failed (attempt {}/{}): {err}, retrying after {}ms...",
                        self.table.table.schema,
                        self.table.table.name,
                        attempt,
                        max_retries,
                        backoff.as_millis(),
                    );
                    status.rows_per_sec = None;
                    status.bytes_per_sec = None;

                    last_error = Some(err.to_string());
                    status.stage = TableCopyStage::ErrorBackoff;
                    status.last_error = last_error.clone();
                    ctx.set_status(status);

                    if cancel
                        .run_until_cancelled(safe_sleep(backoff))
                        .await
                        .is_none()
                    {
                        return Err(Error::DataSyncAborted);
                    }

                    // FUTURE: truncate before retry to handle the COPY-committed-but-dropped
                    // race (rows remain → PK violations). Safe once source-guard checks exist.
                    if !self.can_retry().await {
                        return Err(err);
                    }
                }
            }
        }
    }
}

impl TableDataSyncTask {
    /// Whether the destination is in a state where a failed copy can be retried.
    /// Without 2PC, a failed copy can leave rows that a retry would collide with.
    async fn can_retry(&self) -> bool {
        if self.dest.two_pc_enabled() {
            return true;
        }

        !validate_destination_has_rows(&self.table, &self.dest).await
    }
}
