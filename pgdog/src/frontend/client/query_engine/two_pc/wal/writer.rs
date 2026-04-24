//! Two-phase commit WAL writer task.
//!
//! Owns the active [`Segment`] and serializes appends from many concurrent
//! callers behind a single fsync per batch (group commit). Callers send a
//! [`WriteRequest`] on the [`Wal`]'s mpsc channel and await a oneshot ack
//! that fires once the record is durable.
//!
//! Batching strategy: when at least one request arrives, the task drains
//! the channel non-blockingly to grab any other immediately-available
//! requests. If the batch is still smaller than [`MAX_BATCH`], the task
//! races a `recv` against a `sleep_until(deadline)` where `deadline` is
//! `fsync_interval` after the first request arrived. Once either limit is
//! hit, the batch is encoded into a single [`BytesMut`], written to the
//! segment in one `write_all`, and a single `sync_all` covers it.
//!
//! Segment rotation happens after the sync of any batch that pushed the
//! current segment over the configured size limit.
//!
//! The writer's body is wrapped in `catch_unwind` so a panic doesn't hang
//! shutdown: any unwinding is logged and the `done` notify still fires.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures::FutureExt;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::time::{sleep_until, Instant};
use tracing::{error, warn};

use super::error::Error;
use super::record::{BeginPayload, Record, TxnPayload};
use super::segment::Segment;
use crate::config::config;
use crate::frontend::client::query_engine::two_pc::TwoPcTransaction;

/// Maximum number of records coalesced into a single fsync.
const MAX_BATCH: usize = 1024;
/// Initial capacity of the batch encoding buffer; grows on demand.
const ENCODE_BUF_INITIAL: usize = 64 * 1024;
/// Channel capacity for incoming write requests.
const CHANNEL_CAPACITY: usize = 1024;

/// One outstanding append request from a [`Wal`] caller.
pub(super) struct WriteRequest {
    pub record: Record,
    pub ack: oneshot::Sender<Result<u64, Arc<Error>>>,
}

#[derive(Debug, Default)]
struct WalShutdown {
    cancel: Notify,
    cancelled: AtomicBool,
    done: Notify,
}

/// Cheaply-cloneable handle to the WAL writer task.
#[derive(Debug, Clone)]
pub struct Wal {
    tx: mpsc::Sender<WriteRequest>,
    shutdown: Arc<WalShutdown>,
}

impl Wal {
    /// Spawn the writer task with `initial` as the active segment.
    pub fn new(initial: Segment) -> Self {
        let (tx, rx) = mpsc::channel::<WriteRequest>(CHANNEL_CAPACITY);
        let shutdown = Arc::new(WalShutdown::default());

        tokio::spawn({
            let shutdown = Arc::clone(&shutdown);

            async move {
                let fut = std::panic::AssertUnwindSafe(run(initial, rx, Arc::clone(&shutdown)));
                if fut.catch_unwind().await.is_err() {
                    error!("2pc wal writer task panicked");
                }
                shutdown.done.notify_waiters();
            }
        });

        Self { tx, shutdown }
    }

    /// Log that `txn` is about to issue PREPARE TRANSACTION on its
    /// participants. Must complete before any PREPARE leaves the
    /// coordinator.
    pub async fn append_begin(
        &self,
        txn: TwoPcTransaction,
        user: String,
        database: String,
    ) -> Result<u64, Arc<Error>> {
        self.append(Record::Begin(BeginPayload {
            txn,
            user,
            database,
        }))
        .await
    }

    /// Log that `txn` has crossed the point of no return: every
    /// participant must now be driven to COMMIT PREPARED.
    pub async fn append_committing(&self, txn: TwoPcTransaction) -> Result<u64, Arc<Error>> {
        self.append(Record::Committing(TxnPayload { txn })).await
    }

    /// Log that `txn` is fully resolved (committed or aborted on every
    /// participant) and recovery may forget it.
    pub async fn append_end(&self, txn: TwoPcTransaction) -> Result<u64, Arc<Error>> {
        self.append(Record::End(TxnPayload { txn })).await
    }

    /// Send a record to the writer task. Resolves once the record (and
    /// any other records in its group-commit batch) have been fsynced.
    async fn append(&self, record: Record) -> Result<u64, Arc<Error>> {
        let (ack, rx) = oneshot::channel();
        self.tx
            .send(WriteRequest { record, ack })
            .await
            .map_err(|_| Arc::new(Error::WriterGone))?;
        rx.await.map_err(|_| Arc::new(Error::WriterGone))?
    }

    /// Signal the writer to stop accepting new records, drain any
    /// in-flight requests, and exit. Resolves once the writer task has
    /// finished (cleanly, via panic, or via a final fsync).
    pub async fn shutdown(&self) {
        let waiter = self.shutdown.done.notified();
        self.shutdown.cancelled.store(true, Ordering::Relaxed);
        self.shutdown.cancel.notify_waiters();
        waiter.await;
    }
}

async fn run(
    mut segment: Segment,
    mut rx: mpsc::Receiver<WriteRequest>,
    shutdown: Arc<WalShutdown>,
) {
    let mut batch: Vec<WriteRequest> = Vec::with_capacity(MAX_BATCH);
    let mut encode_buf = BytesMut::with_capacity(ENCODE_BUF_INITIAL);

    loop {
        if shutdown.cancelled.load(Ordering::Relaxed) {
            // Drain any remaining requests, fsync them, exit.
            while let Ok(req) = rx.try_recv() {
                batch.push(req);
            }
            if !batch.is_empty() {
                process_batch(&mut segment, &mut batch, &mut encode_buf).await;
            }
            return;
        }

        // Wait for the first request or a wake from shutdown.
        let first = tokio::select! {
            biased;
            _ = shutdown.cancel.notified() => continue,
            req = rx.recv() => req,
        };
        let Some(first) = first else { return };
        batch.push(first);
        let deadline = Instant::now()
            + Duration::from_millis(config().config.general.two_phase_commit_wal_fsync_interval);

        // Greedy drain of immediately-available requests, no yields.
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(req) => batch.push(req),
                Err(_) => break,
            }
        }

        // Race more recv against the deadline (or cancellation).
        while batch.len() < MAX_BATCH {
            tokio::select! {
                biased;
                _ = shutdown.cancel.notified() => break,
                _ = sleep_until(deadline) => break,
                req = rx.recv() => match req {
                    Some(req) => batch.push(req),
                    None => break,
                }
            }
        }

        process_batch(&mut segment, &mut batch, &mut encode_buf).await;

        if segment.size_bytes() >= config().config.general.two_phase_commit_wal_segment_size {
            match Segment::create(
                &config().config.general.two_phase_commit_wal_dir,
                segment.next_lsn(),
            )
            .await
            {
                Ok(new_seg) => segment = new_seg,
                Err(err) => {
                    error!(
                        "2pc wal: failed to rotate segment at lsn {}: {}",
                        segment.next_lsn(),
                        err
                    );
                    // Keep using the over-sized segment; it still works.
                }
            }
        }
    }
}

async fn process_batch(
    segment: &mut Segment,
    batch: &mut Vec<WriteRequest>,
    encode_buf: &mut BytesMut,
) {
    encode_buf.clear();
    let mut encode_err: Option<Error> = None;
    for req in batch.iter() {
        if let Err(err) = req.record.encode(encode_buf) {
            encode_err = Some(err);
            break;
        }
    }

    if let Some(err) = encode_err {
        warn!("2pc wal: encode failed for batch: {}", err);
        let shared = Arc::new(err);
        for req in batch.drain(..) {
            let _ = req.ack.send(Err(Arc::clone(&shared)));
        }
        return;
    }

    let count = batch.len() as u32;
    let result = match segment.append_batch(encode_buf, count).await {
        Ok(start) => match segment.sync().await {
            Ok(()) => Ok(start),
            Err(err) => Err(err),
        },
        Err(err) => Err(err),
    };

    match result {
        Ok(start) => {
            for (i, req) in batch.drain(..).enumerate() {
                let _ = req.ack.send(Ok(start + i as u64));
            }
        }
        Err(err) => {
            warn!("2pc wal: write/sync failed for batch: {}", err);
            let shared = Arc::new(err);
            for req in batch.drain(..) {
                let _ = req.ack.send(Err(Arc::clone(&shared)));
            }
        }
    }
}
