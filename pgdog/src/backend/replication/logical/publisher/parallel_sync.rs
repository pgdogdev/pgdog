//! Sync tables in parallel up to maximum number of workers concurrency.
//!
//! Each table uses its own replication slot for determining LSN,
//! so make sure there are enough replication slots available (`max_replication_slots` setting).
//!
use std::sync::Arc;

use tokio::{
    spawn,
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        Semaphore,
    },
    task::JoinHandle,
    time::sleep,
};
use tracing::{info, warn};

use super::super::Error;
use super::AbortSignal;
use crate::backend::{
    pool::Address,
    replication::{publisher::Table, status::TableCopy},
    Cluster, Pool,
};

struct ParallelSync {
    table: Table,
    addr: Address,
    dest: Cluster,
    tx: UnboundedSender<Result<Table, Error>>,
    permit: Arc<Semaphore>,
}

impl ParallelSync {
    // Run parallel sync.
    pub fn run(mut self) -> JoinHandle<Result<(), Error>> {
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

            if self.tx.is_closed() {
                return Err(Error::DataSyncAborted);
            }

            self.run_with_retry(&tracker).await
        })
    }

    /// Retry loop: attempt the table copy up to `max_retries` times.
    /// Abort signals and schema errors are not retried.
    async fn run_with_retry(&mut self, tracker: &TableCopy) -> Result<(), Error> {
        let max_retries = self.dest.resharding_copy_retry_max_attempts();
        let base_delay = *self.dest.resharding_copy_retry_min_delay();
        let mut attempt = 0usize;

        loop {
            let abort = AbortSignal::new(self.tx.clone());

            match self
                .table
                .data_sync(&self.addr, &self.dest, abort, tracker)
                .await
            {
                Ok(_) => {
                    self.tx
                        .send(Ok(self.table.clone()))
                        .map_err(|_| Error::ParallelConnection)?;
                    return Ok(());
                }
                Err(err) if !err.is_retryable() || attempt >= max_retries => {
                    tracker.error(&err);
                    // COPY is usually atomic, but rows may remain if the connection dropped
                    // after COMMIT. Warn so the user can truncate manually before retrying.
                    match self.table.destination_has_rows(&self.dest).await {
                        Ok(true) => warn!(
                            "data sync for \"{}\".\"{}\" failed with rows remaining in destination; \
                             truncate manually before retrying: TRUNCATE \"{}\".\"{}\";",
                            self.table.table.schema,
                            self.table.table.name,
                            self.table.table.destination_schema(),
                            self.table.table.destination_name(),
                        ),
                        Ok(false) => {} // destination is clean; next run starts fresh
                        Err(check_err) => warn!(
                            "could not check destination row count for \"{}\".\"{}\" after failure: {check_err}",
                            self.table.table.schema,
                            self.table.table.name,
                        ),
                    }
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
                    // FUTURE: truncate before retry to handle the COPY-committed-but-dropped
                    // race (rows remain → PK violations). Safe once source-guard checks exist.
                }
            }
        }
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
    pub fn new(tables: Vec<Table>, replicas: Vec<Pool>, dest: &Cluster) -> Result<Self, Error> {
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
            dest: dest.clone(),
        })
    }

    /// Run parallel table sync and return table LSNs when everything is done.
    pub async fn run(self) -> Result<Vec<Table>, Error> {
        info!(
            "starting parallel table copy using {} replicas and {} parallel copies",
            self.replicas.len(),
            self.permit.available_permits() / self.replicas.len(),
        );

        // cycle() is the idiomatic "rewind": it restarts the iterator from the
        // beginning once exhausted, giving round-robin distribution across replicas.
        let mut replicas_iter = self.replicas.iter().cycle();

        let (tx, mut rx) = unbounded_channel();
        let mut tables = vec![];
        let mut handles = vec![];

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
                    tx: tx.clone(),
                    permit: self.permit.clone(),
                }
                .run(),
            );
        }

        drop(tx);

        while let Some(table) = rx.recv().await {
            tables.push(table?);
        }

        for handle in handles {
            handle.await??;
        }

        Ok(tables)
    }
}
