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
use crate::backend::replication::logical::orchestrator::Orchestrator;
use crate::backend::replication::publisher::{Table, resolve_resharding_replicas};
use crate::frontend::client::query_engine::two_pc::Manager;
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
use tracing::{info, warn};

const STATUS_REPORT_INTERVAL: Duration = Duration::from_secs(1);
const LOG_REPORT_INTERVAL: Duration = Duration::from_secs(5);

/// Bulk-copy table data from a source database to a target.
///
/// # Invariants
///
/// This task does only the copy of tables without creating replication slots by itself.
/// The replication slots to preserve any updates while copy is in progress should be created
/// upfront before calling this task.
#[derive(Debug, bon::Builder)]
pub(crate) struct CopyDataTask {
    pub(crate) orchestrator: Orchestrator,
    pub(crate) format: CopyFormat,
    /// Require a usable replica identity per table. Only streaming needs it,
    /// so a sync-only migration passes `false`. See `Publisher::data_sync`.
    pub(crate) require_replica_identity: bool,
}

impl Task for CopyDataTask {
    type Status = CopyDataStatus;
    type Output = ();
    type Error = Error;

    fn definition(&self) -> impl Into<TaskDefinition> {
        CopyDataDefinition {
            databases: self.orchestrator.databases(),
            format: self.format,
        }
    }

    async fn run(self, ctx: TaskContext<Self>) -> Result<(), Error> {
        let source = &self.orchestrator.source;
        let dest = &self.orchestrator.destination;
        let mut publisher = self.orchestrator.publisher().await;
        let cancel = ctx.cancellation_token();

        ctx.set_status(CopyDataStage::LoadingTableMetadata.into());
        publisher.sync_tables(true, source).await?;

        if self.require_replica_identity {
            ctx.set_status(CopyDataStage::ValidatingTables.into());
            validate_replica_identity(&publisher.tables)?;
        }

        ctx.set_status(CopyDataStage::CreatingSlots.into());
        publisher.create_slots(source, &cancel).await?;

        let tables = &publisher.tables;
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

        let guard = cancel.drop_guard_ref();

        let mut handles = FuturesUnordered::new();

        for shard in source.shards().iter() {
            let tables = tables.get(&shard.number()).cloned().unwrap_or_default();

            info!(
                "table sync starting for {} tables, shard={}",
                tables.len(),
                shard.number()
            );

            let resharding_replicas = resolve_resharding_replicas(shard);

            let pool = Arc::new(WorkerPool::new(
                resharding_replicas,
                NonZeroUsize::new(dest.resharding_parallel_copies())
                    .unwrap_or(NonZeroUsize::new(1).unwrap()),
            )?);

            for table in tables {
                let pool = Arc::clone(&pool);
                let source = source.clone();
                let dest = dest.clone();
                let ctx = ctx.clone();
                let shard_number = shard.number();
                let format = self.format;
                // W: should we even use tasks for it?
                handles.push(tasks::spawn("tables copy", async move {
                    let table_sync_task = TableDataSyncTask {
                        pool,
                        table,
                        source,
                        dest,
                        format,
                        source_shard: shard_number,
                    };

                    let table = ctx.run(table_sync_task).await?;

                    Ok::<(usize, Table), Error>((shard_number, table))
                }));
            }
        }

        let mut result: HashMap<usize, Vec<Table>> = HashMap::new();
        while let Some(joined) = handles.next().await {
            let (number, table) = joined??;
            result.entry(number).or_default().push(table);
        }
        guard.disarm();

        publisher.post_data_sync(result);

        Ok(())
    }
}

#[derive(Debug)]
struct TableDataSyncTask {
    pool: Arc<WorkerPool<Address>>,
    table: Table,
    source: Cluster,
    dest: Cluster,
    format: CopyFormat,
    source_shard: usize,
}

impl Task for TableDataSyncTask {
    type Status = TableCopyStatus;

    // W: table?
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
                    let _ = validate_destination_has_rows(&self.table, &self.dest).await;
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

                    safe_sleep(backoff).await;

                    if self.dest.two_pc_enabled()
                        && let Some(txn) = err.two_pc_cleanup_transaction()
                    {
                        Manager::get().wait_until_cleaned_up(txn).await;
                    }

                    // Not idempotent (no truncate): if a prior attempt left rows on a
                    // reachable shard, the re-copy can only collide, so stop instead of
                    // retrying. A shard we cannot probe is logged (not blocked on), since we
                    // cannot prove it dirty. Checked after the backoff so a RELOAD-churned
                    // pool can recover first.
                    // FUTURE: truncate before retry to handle the COPY-committed-but-dropped
                    // race (rows remain → PK violations). Safe once source-guard checks exist.
                    if validate_destination_has_rows(&self.table, &self.dest).await {
                        return Err(err);
                    }
                }
            }
        }
    }
}
