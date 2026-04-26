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

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures::FutureExt;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::time::{sleep_until, Instant};
use tracing::{error, warn};

use fnv::FnvHashMap as HashMap;

use super::error::Error;
use super::record::{BeginPayload, CheckpointEntry, CheckpointPayload, Record, TxnPayload};
use super::recovery;
use super::segment::{gc_before_lsn, Segment};
use crate::config::config;
use crate::frontend::client::query_engine::two_pc::{Manager, TwoPcTransaction};

/// Acquire an exclusive flock on `<dir>/.lock` and stamp it with our
/// PID + start time. Returns the locked `File`; dropping it releases
/// the lock. If another process holds the lock, the existing file is
/// read back and surfaced verbatim in [`Error::DirLocked`] so an
/// operator can see who's holding it.
fn lock_dir(dir: &Path) -> Result<std::fs::File, Error> {
    let lock_path = dir.join(".lock");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|source| Error::DirNotAccessible {
            dir: dir.to_path_buf(),
            source,
        })?;

    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            let mut holder = String::new();
            let _ = file.read_to_string(&mut holder);
            return Err(Error::DirLocked {
                dir: dir.to_path_buf(),
                holder,
            });
        }
        Err(std::fs::TryLockError::Error(source)) => {
            return Err(Error::DirNotWritable {
                dir: dir.to_path_buf(),
                source,
            });
        }
    }

    let stamp = format!(
        "pid={}\nstarted={}\n",
        std::process::id(),
        chrono::Utc::now().to_rfc3339(),
    );
    file.set_len(0).map_err(|source| Error::DirNotWritable {
        dir: dir.to_path_buf(),
        source,
    })?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| Error::DirNotWritable {
            dir: dir.to_path_buf(),
            source,
        })?;
    file.write_all(stamp.as_bytes())
        .map_err(|source| Error::DirNotWritable {
            dir: dir.to_path_buf(),
            source,
        })?;
    Ok(file)
}

/// Maximum number of records coalesced into a single fsync.
const MAX_BATCH: usize = 1024;
/// Initial capacity of the batch encoding buffer; grows on demand.
const ENCODE_BUF_INITIAL: usize = 64 * 1024;
/// Channel capacity for incoming write requests.
const CHANNEL_CAPACITY: usize = 1024;

/// What a [`WriteRequest`] asks the writer to append.
pub(super) enum WalAppend {
    /// Append this record verbatim.
    Record(Record),
    /// Materialize a [`Record::Checkpoint`] from the writer's snapshot
    /// at this point in the batch, then append it.
    Checkpoint,
}

/// One outstanding append request from a [`Wal`] caller.
pub(super) struct WriteRequest {
    pub append: WalAppend,
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
    /// Probe the configured WAL directory, take an exclusive flock on
    /// `<dir>/.lock` so a second pgdog can't race us, replay any
    /// existing log into `manager`, and spawn the writer task. The
    /// flock is held for the lifetime of the writer task; on shutdown
    /// or panic the underlying `File` drops and the kernel releases
    /// the lock.
    ///
    /// Returns `Err` if the directory isn't usable, another pgdog
    /// already holds the dir, or recovery fails; the caller is
    /// responsible for deciding whether to continue running without
    /// WAL durability.
    pub async fn open(manager: &Manager) -> Result<Self, Error> {
        let dir = &config().config.general.two_phase_commit_wal_dir;
        // Ensure the dir exists before lock_dir tries to open .lock in it.
        tokio::fs::create_dir_all(dir)
            .await
            .map_err(|source| Error::DirNotAccessible {
                dir: dir.to_path_buf(),
                source,
            })?;
        let lock = lock_dir(dir)?;
        let recovered = recovery::recover_transactions(manager, dir).await?;

        let (tx, rx) = mpsc::channel::<WriteRequest>(CHANNEL_CAPACITY);
        let shutdown = Arc::new(WalShutdown::default());

        tokio::spawn({
            let shutdown = Arc::clone(&shutdown);

            async move {
                let fut = std::panic::AssertUnwindSafe(run(
                    recovered.segment,
                    recovered.snapshot,
                    rx,
                    Arc::clone(&shutdown),
                    lock,
                ));
                if fut.catch_unwind().await.is_err() {
                    error!("2pc wal writer task panicked");
                }
                shutdown.done.notify_waiters();
            }
        });

        Ok(Self { tx, shutdown })
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

    /// Snapshot the active 2PC set into a [`Record::Checkpoint`] and
    /// garbage-collect any segment fully superseded by it. The snapshot
    /// is taken inside the writer task so any records ahead of this
    /// marker in the same group-commit batch are reflected. Returns the
    /// LSN of the checkpoint record.
    pub async fn checkpoint(&self) -> Result<u64, Arc<Error>> {
        let (ack, rx) = oneshot::channel();
        self.tx
            .send(WriteRequest {
                append: WalAppend::Checkpoint,
                ack,
            })
            .await
            .map_err(|_| Arc::new(Error::WriterGone))?;
        rx.await.map_err(|_| Arc::new(Error::WriterGone))?
    }

    /// Send a record to the writer task. Resolves once the record (and
    /// any other records in its group-commit batch) have been fsynced.
    async fn append(&self, record: Record) -> Result<u64, Arc<Error>> {
        let (ack, rx) = oneshot::channel();
        self.tx
            .send(WriteRequest {
                append: WalAppend::Record(record),
                ack,
            })
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
    mut snapshot: HashMap<TwoPcTransaction, CheckpointEntry>,
    mut rx: mpsc::Receiver<WriteRequest>,
    shutdown: Arc<WalShutdown>,
    // Held for the writer's lifetime so the kernel keeps our flock on
    // `<dir>/.lock`; dropped when this function returns or panics.
    _dir_lock: std::fs::File,
) {
    let mut batch: Vec<WriteRequest> = Vec::with_capacity(MAX_BATCH);
    let mut encode_buf = BytesMut::with_capacity(ENCODE_BUF_INITIAL);
    // Holds the in-flight GC if one is still running; awaited before
    // the next GC spawns so segment GCs never overlap.
    let mut gc_handle: Option<tokio::task::JoinHandle<()>> = None;

    loop {
        if shutdown.cancelled.load(Ordering::Relaxed) {
            // Drain any remaining requests, fsync them, exit.
            while let Ok(req) = rx.try_recv() {
                batch.push(req);
            }
            if !batch.is_empty() {
                // Drain-on-shutdown: process_batch acks the callers and
                // warns on its own; the rotate/gc signal in the result
                // has nothing to act on because we're about to return,
                // and recovery handles whatever state we leave on disk.
                let _ =
                    process_batch(&mut segment, &mut batch, &mut encode_buf, &mut snapshot).await;
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

        let outcome = process_batch(&mut segment, &mut batch, &mut encode_buf, &mut snapshot).await;

        match outcome {
            Ok(Some(lsn)) => {
                // Run GC off the writer task; filesystem work shouldn't
                // hold up the next batch. Chain through `gc_handle` so two
                // GCs never race on the same directory: the new task awaits
                // the previous one before starting.
                let prev = gc_handle.take();
                gc_handle = Some(tokio::spawn(async move {
                    if let Some(prev) = prev {
                        let _ = prev.await;
                    }
                    let dir = &config().config.general.two_phase_commit_wal_dir;
                    if let Err(err) = gc_before_lsn(dir, lsn).await {
                        warn!("[2pc] wal checkpoint gc failed: {}", err);
                    }
                }));
            }
            Ok(None) => {}
            Err(ref err) if matches!(**err, Error::SegmentBroken) => {
                // Broken segment can't take more writes. Failing to
                // create a replacement here means the disk is gone
                // and we have nowhere to log: panic.
                segment = Segment::create(
                    &config().config.general.two_phase_commit_wal_dir,
                    segment.next_lsn(),
                )
                .await
                .expect("2pc wal: cannot create new segment after segment broken; disk unusable");
            }
            Err(_) => {}
        }

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
    snapshot: &mut HashMap<TwoPcTransaction, CheckpointEntry>,
) -> Result<Option<u64>, Arc<Error>> {
    encode_buf.clear();
    let mut encode_err: Option<Error> = None;
    let mut last_checkpoint_idx: Option<usize> = None;
    // Records every snapshot mutation in batch order so we can roll
    // back to the start-of-batch state if encode or sync fails. Sized
    // O(batch), independent of the active set.
    let mut undo: Vec<Undo> = Vec::with_capacity(batch.len());

    for (i, req) in batch.iter().enumerate() {
        let result = match &req.append {
            WalAppend::Record(r) => {
                apply_to_snapshot(snapshot, r, &mut undo);
                r.encode(encode_buf)
            }
            WalAppend::Checkpoint => {
                last_checkpoint_idx = Some(i);
                Record::Checkpoint(CheckpointPayload {
                    active: snapshot.values().cloned().collect(),
                })
                .encode(encode_buf)
            }
        };
        if let Err(err) = result {
            encode_err = Some(err);
            break;
        }
    }

    if let Some(err) = encode_err {
        replay_undo(snapshot, undo);
        warn!("2pc wal: encode failed for batch: {}", err);
        let shared = Arc::new(err);
        for req in batch.drain(..) {
            let _ = req.ack.send(Err(Arc::clone(&shared)));
        }
        return Err(shared);
    }

    let count = batch.len() as u32;
    match segment.commit(encode_buf, count).await {
        Ok(start) => {
            for (i, req) in batch.drain(..).enumerate() {
                let _ = req.ack.send(Ok(start + i as u64));
            }
            Ok(last_checkpoint_idx.map(|i| start + i as u64))
        }
        Err(err) => {
            replay_undo(snapshot, undo);
            warn!("2pc wal: write/sync failed for batch: {}", err);
            let shared = Arc::new(err);
            for req in batch.drain(..) {
                let _ = req.ack.send(Err(Arc::clone(&shared)));
            }
            Err(shared)
        }
    }
}

/// One snapshot mutation captured for rollback.
struct Undo {
    txn: TwoPcTransaction,
    /// Value the txn held before the mutation; `None` means the txn
    /// wasn't in the snapshot at all and should be removed on undo.
    prior: Option<CheckpointEntry>,
}

/// Apply `record` to `snapshot` and push an entry onto `undo` capturing
/// what to revert to if the batch later fails.
fn apply_to_snapshot(
    snapshot: &mut HashMap<TwoPcTransaction, CheckpointEntry>,
    record: &Record,
    undo: &mut Vec<Undo>,
) {
    match record {
        Record::Begin(p) => {
            let prior = snapshot.insert(
                p.txn,
                CheckpointEntry {
                    txn: p.txn,
                    user: p.user.clone(),
                    database: p.database.clone(),
                    decided: false,
                },
            );
            undo.push(Undo { txn: p.txn, prior });
        }
        Record::Committing(p) => {
            if let Some(entry) = snapshot.get_mut(&p.txn) {
                let prior = entry.clone();
                entry.decided = true;
                undo.push(Undo {
                    txn: p.txn,
                    prior: Some(prior),
                });
            }
        }
        Record::End(p) => {
            if let Some(prior) = snapshot.remove(&p.txn) {
                undo.push(Undo {
                    txn: p.txn,
                    prior: Some(prior),
                });
            }
        }
        Record::Checkpoint(_) => {}
    }
}

/// Replay `undo` in reverse to restore `snapshot` to its state before
/// the batch started.
fn replay_undo(snapshot: &mut HashMap<TwoPcTransaction, CheckpointEntry>, undo: Vec<Undo>) {
    for u in undo.into_iter().rev() {
        match u.prior {
            Some(entry) => {
                snapshot.insert(u.txn, entry);
            }
            None => {
                snapshot.remove(&u.txn);
            }
        }
    }
}
