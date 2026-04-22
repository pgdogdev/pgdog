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
};
use tracing::info;

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
            let _permit = self
                .permit
                .acquire()
                .await
                .map_err(|_| Error::DataSyncAborted)?;

            if self.tx.is_closed() {
                return Err(Error::DataSyncAborted);
            }

            let abort = AbortSignal::new(self.tx.clone());

            let result = match self
                .table
                .data_sync(&self.addr, &self.dest, abort, &tracker)
                .await
            {
                Ok(_) => Ok(self.table),
                Err(err) => {
                    tracker.error(&err);
                    return Err(err);
                }
            };

            self.tx.send(result).map_err(|_| Error::DataSyncAborted)?;

            Ok::<(), Error>(())
        })
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

        let mut replicas_iter = self.replicas.iter();
        // Loop through replicas, one at a time.
        // This works around Rust iterators not having a "rewind" function.
        let replica = loop {
            if let Some(replica) = replicas_iter.next() {
                break replica;
            } else {
                replicas_iter = self.replicas.iter();
            }
        };

        let (tx, mut rx) = unbounded_channel();
        let mut tables = vec![];
        let mut handles = vec![];

        for table in self.tables {
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
