use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::select;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::try_join;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::super::{Error, publisher::Table};
use super::ReplicationSlot;

use crate::backend::replication::logical::publisher::ReplicationData;
use crate::backend::replication::logical::subscriber::stream::StreamSubscriber;
use crate::backend::replication::publisher::Lsn;
use crate::backend::replication::publisher::progress::Progress;
use crate::backend::replication::tables_sync::tables_sync;
use crate::backend::{Cluster, pool::Request};
use crate::net::replication::ReplicationMeta;
use crate::tasks;
use crate::util::{safe_interval, safe_sleep};

#[derive(Debug, Default)]
pub(crate) struct Publisher {
    /// Name of the publication.
    publication: String,
    /// Shard -> Tables mapping.
    pub(crate) tables: HashMap<usize, Vec<Table>>,
    /// Replication slots.
    slots: HashMap<usize, ReplicationSlot>,
    /// Replication lag.
    replication_lag: Arc<Mutex<HashMap<usize, i64>>>,
    /// Last transaction.
    last_transaction: Arc<Mutex<Option<Instant>>>,
    /// Slot name.
    slot_name: String,
}

impl Publisher {
    pub(crate) fn new(publication: &str, slot_name: String) -> Self {
        Self {
            publication: publication.to_string(),
            tables: HashMap::new(),
            slots: HashMap::new(),
            replication_lag: Arc::new(Mutex::new(HashMap::new())),
            last_transaction: Arc::new(Mutex::new(None)),
            slot_name,
        }
    }

    /// Synchronize tables for all shards.
    pub(crate) async fn sync_tables(
        &mut self,
        data_sync: bool,
        source: &Cluster,
    ) -> Result<(), Error> {
        let mut tables = tables_sync(source, source.sharded_tables(), &self.publication).await?;

        if !data_sync {
            // fill out lsns from the existing tables if any
            // TODO: make it explicit before running replication after copy_data task
            for (shard, tables) in &mut tables {
                for table in tables {
                    let existing = self
                        .tables
                        .get(shard)
                        .and_then(|tables| tables.iter().find(|t| table.key_ref() == t.key_ref()));
                    if let Some(existing) = existing {
                        table.lsn = existing.lsn;
                    }
                }
            }
        }

        self.tables = tables;

        Ok(())
    }

    /// Create permanent slots for each shard.
    /// This uses a dedicated connection.
    ///
    /// N.B.: These are not synchronized across multiple shards.
    /// If you're doing a cross-shard transaction, parts of it can be lost.
    ///
    /// TODO: Add support for 2-phase commit.
    pub(crate) async fn create_slots(
        &mut self,
        source: &Cluster,
        cancel: &CancellationToken,
    ) -> Result<(), Error> {
        for (number, shard) in source.shards().iter().enumerate() {
            // Cancel at slot boundaries so we never tear down an in-flight
            // CREATE_REPLICATION_SLOT: the current slot completes, the next is
            // not started. Slots already created are dropped by the caller.
            if cancel.is_cancelled() {
                return Err(Error::DataSyncAborted);
            }

            let addr = shard.primary(&Request::default()).await?.addr().clone();

            let mut slot = ReplicationSlot::replication(
                &self.publication,
                &addr,
                Some(self.slot_name.clone()),
                number,
            );
            Box::pin(slot.create_slot()).await?;

            self.slots.insert(number, slot);
        }

        Ok(())
    }

    /// Replicate and fan-out data from a shard to N shards.
    ///
    /// This uses a dedicated replication slot which will survive crashes and reboots.
    /// N.B.: The slot needs to be manually dropped!
    pub(crate) async fn replicate(
        &mut self,
        source: &Cluster,
        dest: &Cluster,
    ) -> Result<Waiter, Error> {
        // Replicate shards in parallel.
        let mut streams = vec![];

        let stop = CancellationToken::new();

        // Synchronize tables from publication.
        self.sync_tables(false, source).await?;

        // Create replication slots if we haven't already.
        if self.slots.is_empty() {
            Box::pin(self.create_slots(source, &stop)).await?;
        }

        for (number, _) in source.shards().iter().enumerate() {
            // Use table offsets from data sync
            // or from loading them above.
            let tables = self
                .tables
                .get(&number)
                .map(Vec::as_slice)
                .unwrap_or_default();

            let mut stream = StreamSubscriber::new(dest, tables);

            // Take ownership of the slot for replication.
            let mut slot = self
                .slots
                .remove(&number)
                .ok_or(Error::NoReplicationSlot(number))?;
            stream.set_current_lsn(slot.lsn().lsn);

            let mut check_lag = safe_interval(Duration::from_secs(1));
            let replication_lag = self.replication_lag.clone();
            let stop = stop.clone();
            let last_transaction = self.last_transaction.clone();

            let source_cluster = source.clone();
            let dest = dest.clone();

            // Replicate in parallel.
            let handle = tasks::spawn("replication", async move {
                slot.start_replication().await?;
                let progress = Progress::new_stream();
                let max_attempts = dest.resharding_replication_retry_max_attempts();
                let delay = dest.resharding_replication_retry_min_delay();
                let mut attempt = 0usize;
                // Latches on the first cancellation so the `cancelled()` arm fires
                // once (it stays ready forever after `cancel()`); the drain below
                // then runs to completion.
                let mut stopping = false;
                loop {
                    select! {
                        _ = stop.cancelled(), if !stopping => {
                            slot.stop_replication().await?;
                            stopping = true;
                        }

                        // This is cancellation-safe.
                        replication_data = slot.replicate(Duration::MAX) => {
                            // Returns Ok(true) when the slot is drained and the loop
                            // should break; Ok(false) to continue. All errors bubble up
                            // to the single retry/abort site below.
                            let done: Result<bool, Error> = async {
                                let Some(replication_data) = replication_data? else {
                                    slot.drop_slot().await?;
                                    return Ok(true);
                                };
                                match replication_data {
                                    ReplicationData::CopyData(data) => {
                                        if let Some(ReplicationMeta::KeepAlive(ka)) =
                                            data.replication_meta()
                                        {
                                            // Advance the lsn if we are not in the transaction currently
                                            // (we don't use transactions actually without streaming on protocol version 4,
                                            // but let it be as a safeguard).
                                            // If we got the keep-alive message and not the update message
                                            // then it's for the unrelated changes that advanced WAL.
                                            // Since it's unrelated we can advance our progress and
                                            // consider that lag replication
                                            let advanced = !stream.in_transaction()
                                                && stream.set_current_lsn(ka.wal_end);

                                            // Reply to walsender if it asked for reply or
                                            // if we advanced due to the WAL progress but
                                            // the update was not related
                                            if advanced || ka.reply() {
                                                slot.status_update(stream.status_update()).await?;
                                            }
                                            debug!(
                                                "origin at lsn {} [{}]",
                                                Lsn::from_i64(ka.wal_end),
                                                slot.server()?.addr()
                                            );
                                            progress.update(stream.bytes_sharded(), ka.wal_end);
                                        } else {
                                            if let Some(su) = stream.handle(data).await? {
                                                slot.status_update(su).await?;
                                                *last_transaction.lock() = Some(Instant::now());
                                            }
                                            attempt = 0;
                                            progress.update(stream.bytes_sharded(), stream.lsn());
                                        }
                                        Ok(false)
                                    }
                                    ReplicationData::CopyDone => Ok(false),
                                }
                            }
                            .await;

                            match done {
                                Ok(true) => break,
                                Ok(false) => {}
                                Err(err)
                                    if err.is_retryable()
                                        && (max_attempts == 0 || attempt < max_attempts) =>
                                {
                                    attempt += 1;
                                    warn!(
                                        "[replication] error ({attempt}/{max_attempts}): {err}, reconnecting in {}ms",
                                        delay.as_millis()
                                    );
                                    safe_sleep(delay).await;
                                    if let Err(reconnect_err) =
                                        try_join!(slot.reconnect(), stream.reconnect())
                                    {
                                        if !reconnect_err.is_retryable() {
                                            return Err(reconnect_err);
                                        }
                                        stream.reset_connections();
                                        warn!(
                                            "[replication] reconnect error ({attempt}/{max_attempts}): {reconnect_err}, will retry"
                                        );
                                    }
                                }
                                Err(err) => return Err(err),
                            }
                        }

                        _ = check_lag.tick() => {
                            let lag = slot.replication_lag().await?;

                            let mut guard = replication_lag.lock();
                            guard.insert(number, lag);

                            let missed = stream.missed_rows();
                            if missed.non_zero() {
                                warn!("replication {} => {} has missing rows: {}", source_cluster.name(), dest.name(), missed);
                            }

                        }
                    }
                }

                Ok::<(), Error>(())
            });

            streams.push(handle);
        }

        Ok(Waiter { streams, stop })
    }

    /// Get current replication lag.
    pub(crate) fn replication_lag(&self) -> HashMap<usize, i64> {
        self.replication_lag.lock().clone()
    }

    /// Get how long ago last transaction was committed.
    pub(crate) fn last_transaction(&self) -> Option<Duration> {
        (*self.last_transaction.lock()).map(|last| last.elapsed())
    }

    pub(crate) fn post_data_sync(&mut self, tables: HashMap<usize, Vec<Table>>) {
        self.tables = tables;
    }

    /// Drop the replication slots created during data sync.
    ///
    /// Idempotent: the slot map is taken out up front, so repeated calls — or a
    /// call after replication already took the slots over — are no-ops. Every
    /// slot is attempted even if one fails; the first error is returned.
    pub(crate) async fn cleanup(&mut self) -> Result<(), Error> {
        let mut error = None;
        for (_, mut slot) in std::mem::take(&mut self.slots) {
            if let Err(err) = slot.drop_slot().await {
                error.get_or_insert(err);
            }
        }

        error.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
impl Publisher {
    pub(crate) fn set_replication_lag(&self, shard: usize, lag: i64) {
        self.replication_lag.lock().insert(shard, lag);
    }

    pub(crate) fn set_last_transaction(&self, instant: Option<Instant>) {
        *self.last_transaction.lock() = instant;
    }
}

#[derive(Debug)]
pub(crate) struct Waiter {
    streams: Vec<JoinHandle<Result<(), Error>>>,
    stop: CancellationToken,
}

impl Waiter {
    pub(crate) fn stop(&self) {
        self.stop.cancel();
    }

    pub(crate) async fn wait(&mut self) -> Result<(), Error> {
        for stream in &mut self.streams {
            stream.await??;
        }

        Ok(())
    }
}

#[cfg(test)]
impl Waiter {
    pub(crate) fn new_test() -> Self {
        Self {
            streams: vec![],
            stop: CancellationToken::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::backend::server::test::test_replication_server;
    use crate::config::config;

    /// A pre-cancelled token aborts slot creation before the first slot,
    /// leaving the publisher without slots.
    #[tokio::test]
    async fn create_slots_aborts_on_cancelled_token() {
        crate::logger();

        let mut server = test_replication_server().await;
        for ddl in &[
            "CREATE TABLE IF NOT EXISTS publication_test_sync_only_no_pk (data TEXT NOT NULL)",
            "DROP PUBLICATION IF EXISTS publication_sync_only_no_pk",
            "CREATE PUBLICATION publication_sync_only_no_pk FOR TABLE publication_test_sync_only_no_pk",
        ] {
            server.execute(*ddl).await.unwrap();
        }

        let source = Cluster::new_test(&config());
        source.launch();

        let mut publisher =
            Publisher::new("publication_sync_only_no_pk", "sync_only_no_pk_slot".into());

        let cancel = CancellationToken::new();
        cancel.cancel();
        publisher.sync_tables(true, &source).await.unwrap();
        let result = publisher.create_slots(&source, &cancel).await;

        assert!(
            matches!(result, Err(Error::DataSyncAborted)),
            "slot creation must abort on a cancelled token; got: {result:?}"
        );
        assert!(
            publisher.slots.is_empty(),
            "the cancelled token aborts before any slot is created"
        );

        source.shutdown();
        for ddl in &[
            "DROP PUBLICATION IF EXISTS publication_sync_only_no_pk",
            "DROP TABLE IF EXISTS publication_test_sync_only_no_pk",
        ] {
            server.execute(*ddl).await.unwrap();
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    use crate::net::{
        CopyData, ToBytes,
        replication::{
            XLogData,
            logical::{begin::Begin, commit::Commit},
        },
    };
    /// Wrap a Begin payload in an XLogData CopyData message.
    fn begin_copy_data(lsn: i64) -> CopyData {
        let xlog = XLogData {
            starting_point: lsn,
            current_end: lsn,
            system_clock: 0,
            bytes: Begin {
                final_transaction_lsn: lsn,
                commit_timestamp: 0,
                xid: 1,
            }
            .to_bytes(),
        };
        CopyData::new(&xlog.to_bytes())
    }
    fn commit_copy_data(lsn: i64) -> CopyData {
        let xlog = XLogData {
            starting_point: lsn,
            current_end: lsn,
            system_clock: 0,
            bytes: Commit {
                flags: 0,
                commit_lsn: 0,
                end_lsn: lsn,
                commit_timestamp: 0,
            }
            .to_bytes(),
        };
        CopyData::new(&xlog.to_bytes())
    }

    // -- handle ---------------------------------------------------------------

    /// A Begin event produces `Ok(None)` — no status update to forward to the origin.
    #[tokio::test]
    async fn apply_begin_no_status_update() {
        let cfg = config();
        let cluster = Cluster::new_test(&cfg);
        cluster.launch();
        let mut stream = StreamSubscriber::new(&cluster, &[]);
        stream.connect().await.unwrap();

        let result = stream.handle(begin_copy_data(1)).await;

        assert!(
            result.unwrap().is_none(),
            "Begin event must not emit a status update"
        );
        cluster.shutdown();
    }

    /// A Commit event returns `Ok(Some(su))` — the caller must forward the
    /// status update to the replication origin. Distinct from a Begin, which
    /// returns `Ok(None)` and produces no status update.
    #[tokio::test]
    async fn apply_commit_emits_status_update() {
        let cfg = config();
        let cluster = Cluster::new_test(&cfg);
        cluster.launch();
        let mut stream = StreamSubscriber::new(&cluster, &[]);
        stream.connect().await.unwrap();

        let result = stream.handle(commit_copy_data(1)).await;

        // Commit must succeed and produce a status update for the caller to
        // send to the origin via slot.status_update().
        assert!(result.is_ok());
        assert!(
            result.unwrap().is_some(),
            "commit should produce a status update"
        );
        cluster.shutdown();
    }
}
