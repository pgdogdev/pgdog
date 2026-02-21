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
};

use super::super::Error;
use super::AbortSignal;
use crate::backend::{pool::Address, replication::publisher::Table, Cluster, Pool};

struct ParallelSync {
    table: Table,
    addr: Address,
    dest: Cluster,
    tx: UnboundedSender<Result<Table, Error>>,
    permit: Arc<Semaphore>,
    shard: usize,
}

impl ParallelSync {
    // Run parallel sync.
    pub fn run(mut self) {
        spawn(async move {
            // This won't acquire until we have at least 1 available permit.
            // Permit will be given back when this task completes.
            let _permit = self
                .permit
                .acquire()
                .await
                .map_err(|_| Error::ParallelConnection)?;

            if self.tx.is_closed() {
                return Err(Error::DataSyncAborted);
            }

            let abort = AbortSignal::new(self.tx.clone());

            let result = match self.table.data_sync(&self.addr, &self.dest, abort).await {
                Ok(_) => Ok(self.table),
                Err(err) => Err(err),
            };

            self.tx
                .send(result)
                .map_err(|_| Error::ParallelConnection)?;

            Ok::<(), Error>(())
        });
    }
}

/// Sync tables in parallel up to maximum concurrency.
pub struct ParallelSyncManager {
    permit: Arc<Semaphore>,
    tables: Vec<Table>,
    replicas: Vec<Pool>,
    dest: Cluster,
    shard: usize,
}

impl ParallelSyncManager {
    /// Create parallel sync manager.
    pub fn new(
        tables: Vec<Table>,
        replicas: Vec<Pool>,
        dest: &Cluster,
        shard: usize,
    ) -> Result<Self, Error> {
        if replicas.is_empty() {
            return Err(Error::NoReplicas);
        }

        Ok(Self {
            permit: Arc::new(Semaphore::new(replicas.len())),
            tables,
            replicas,
            dest: dest.clone(),
            shard,
        })
    }

    /// Run parallel table sync and return table LSNs when everything is done.
    pub async fn run(self) -> Result<Vec<Table>, Error> {
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

        for table in self.tables {
            ParallelSync {
                table,
                addr: replica.addr().clone(),
                dest: self.dest.clone(),
                tx: tx.clone(),
                permit: self.permit.clone(),
                shard: self.shard,
            }
            .run();
        }

        drop(tx);

        while let Some(table) = rx.recv().await {
            tables.push(table?);
        }

        Ok(tables)
    }
}
