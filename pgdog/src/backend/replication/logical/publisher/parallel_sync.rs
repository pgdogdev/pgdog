//! Sync tables in parallel up to maximum number of workers concurrency.
//!
//! Each table uses its own replication slot for determining LSN,
//! so make sure there are enough replication slots available (`max_replication_slots` setting).
//!
use std::sync::Arc;

use tokio::{spawn, sync::Semaphore, task::JoinHandle, time::sleep};
use tracing::{info, warn};

use super::super::Error;
use crate::backend::{
    Cluster, Pool,
    pool::{Address, Request},
    replication::{publisher::Table, status::TableCopy},
};
use crate::frontend::client::query_engine::two_pc::Manager;
use crate::net::messages::Protocol;
use crate::util::escape_identifier;
use futures::{StreamExt, stream::FuturesUnordered};
use tokio_util::sync::CancellationToken;

struct ParallelSync {
    table: Table,
    addr: Address,
    dest: Cluster,
    permit: Arc<Semaphore>,
    cancel: CancellationToken,
}

impl ParallelSync {
    // Run parallel sync.
    pub fn run(self) -> JoinHandle<Result<Table, Error>> {
        spawn(async move {
            // Record copy in queue before waiting for permit.
            let tracker = TableCopy::new(&self.table.table.schema, &self.table.table.name);

            // This won't acquire until we have at least 1 available permit.
            // Permit will be given back when this task completes.
            // acquire_owned() consumes a cloned Arc, returning an OwnedSemaphorePermit with
            // no lifetime tied to `self`, which allows the subsequent `&mut self` borrow.
            let _permit = Arc::clone(&self.permit)
                .acquire_owned()
                .await
                .map_err(|_| Error::ParallelConnection)?;

            if self.cancel.is_cancelled() {
                return Err(Error::DataSyncAborted);
            }

            self.run_with_retry(&tracker).await
        })
    }

    /// Retry loop: attempt the table copy up to `max_retries` times.
    /// Abort signals and schema errors are not retried.
    async fn run_with_retry(mut self, tracker: &TableCopy) -> Result<Table, Error> {
        let max_retries = self.dest.resharding_copy_retry_max_attempts();
        let base_delay = *self.dest.resharding_copy_retry_min_delay();
        let mut attempt = 0usize;

        loop {
            match self
                .table
                .data_sync(&self.addr, &self.dest, &self.cancel, tracker)
                .await
            {
                Ok(_) => return Ok(self.table),
                Err(err) if !err.is_retryable() || attempt >= max_retries => {
                    tracker.error(&err);
                    // Terminal failure: warn if rows remain so the operator can truncate.
                    let _ = self.destination_has_rows().await;
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

                    // Reset counters so the next attempt's progress is reported accurately.
                    tracker.reset();

                    sleep(backoff).await;

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
                    if self.destination_has_rows().await {
                        return Err(err);
                    }
                }
            }
        }
    }

    /// Returns `true` if any reachable destination shard holds rows from a prior COPY
    /// attempt. Every shard is probed; a shard whose probe errors (e.g. its pool was
    /// shut down by a RELOAD) is logged at WARN and not counted, since we cannot prove
    /// it dirty. Emits a single WARN listing the shards that do hold rows.
    async fn destination_has_rows(&self) -> bool {
        let schema = self.table.table.destination_schema();
        let name = self.table.table.destination_name();
        let sql = format!(
            "SELECT 1 FROM \"{}\".\"{}\" LIMIT 1",
            escape_identifier(schema),
            escape_identifier(name),
        );

        let mut shards_with_rows = vec![];
        for (shard, _) in self.dest.shards().iter().enumerate() {
            let result: Result<bool, Error> = async {
                let mut server = self.dest.primary(shard, &Request::default()).await?;
                Ok(server
                    .execute_checked(sql.as_str())
                    .await?
                    .iter()
                    .any(|m| m.code() == 'D'))
            }
            .await;

            match result {
                Ok(true) => shards_with_rows.push(shard),
                Ok(false) => {}
                Err(err) => warn!(
                    "could not verify destination rows on shard {shard}: {err}; \
                     proceeding as if empty"
                ),
            }
        }

        if !shards_with_rows.is_empty() {
            warn!(
                "destination \"{schema}\".\"{name}\" holds rows from a prior COPY attempt on shard(s) {shards_with_rows:?}; \
                 truncate before re-running the copy: TRUNCATE \"{schema}\".\"{name}\";",
            );
        }

        !shards_with_rows.is_empty()
    }
}

/// Sync tables in parallel up to maximum concurrency.
pub struct ParallelSyncManager {
    permit: Arc<Semaphore>,
    tables: Vec<Table>,
    replicas: Vec<Pool>,
    dest: Cluster,
}

impl ParallelSyncManager {
    /// Create parallel sync manager.
    pub fn new(tables: Vec<Table>, replicas: Vec<Pool>, dest: Cluster) -> Result<Self, Error> {
        if replicas.is_empty() {
            return Err(Error::NoReplicas);
        }

        Ok(Self {
            // TODO: this single shared semaphore cannot enforce per-replica limits — all
            // permits could be consumed by tasks that round-robin happened to assign to the
            // same replica, leaving others idle. Fix: replace with one Semaphore per replica,
            // each sized to `parallel_copies`, and have each ParallelSync acquire from its
            // assigned replica's semaphore.
            permit: Arc::new(Semaphore::new(
                replicas.len() * dest.resharding_parallel_copies(),
            )),
            tables,
            replicas,
            dest,
        })
    }

    /// Run parallel table sync and return table LSNs when everything is done.
    pub async fn run(self, cancel: CancellationToken) -> Result<Vec<Table>, Error> {
        info!(
            "starting parallel table copy using {} replicas and {} parallel copies",
            self.replicas.len(),
            self.permit.available_permits() / self.replicas.len(),
        );

        // Create a child cancel token with the guard to cancel the handles below
        // in case any of it fails without affecting the parent task.
        // If every handle succeed the guard token will just cancel already finished work
        let cancel = cancel.child_token();
        let _guard = cancel.clone().drop_guard();

        // cycle() is the idiomatic "rewind": it restarts the iterator from the
        // beginning once exhausted, giving round-robin distribution across replicas.
        let mut replicas_iter = self.replicas.iter().cycle();

        let mut handles = FuturesUnordered::new();

        for table in self.tables {
            // SAFETY: cycle() on a non-empty slice never returns None.
            let replica = replicas_iter
                .next()
                .expect("replicas is non-empty; checked in new()");
            handles.push(
                ParallelSync {
                    table,
                    addr: replica.addr().clone(),
                    dest: self.dest.clone(),
                    permit: self.permit.clone(),
                    cancel: cancel.clone(),
                }
                .run(),
            );
        }

        let mut tables = Vec::with_capacity(handles.len());

        // Short-circuit on first error and cancel other futures (JoinHandles that are not cancellable on drop)
        // thanks to cancel guard.
        while let Some(joined) = handles.next().await {
            tables.push(joined??);
        }

        Ok(tables)
    }
}
