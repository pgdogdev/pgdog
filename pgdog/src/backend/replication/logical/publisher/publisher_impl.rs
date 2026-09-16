use std::collections::HashMap;

use tokio_util::sync::CancellationToken;

use super::super::{Error, publisher::Table};
use super::ReplicationSlot;
use crate::backend::replication::tables_sync::tables_sync;
use crate::backend::{Cluster, pool::Request};

#[derive(Debug, Default)]
pub(crate) struct Publisher {
    /// Name of the publication.
    publication: String,
    /// Shard -> Tables mapping.
    pub(crate) tables: HashMap<usize, Vec<Table>>,
    /// Replication slots.
    slots: HashMap<usize, ReplicationSlot>,
    slot_name: String,
}

impl Publisher {
    pub(crate) fn new(publication: &str, slot_name: String) -> Self {
        Self {
            publication: publication.to_string(),
            tables: HashMap::new(),
            slots: HashMap::new(),
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

    pub(crate) async fn prepare_replication(
        &mut self,
        source: &Cluster,
        cancel: &CancellationToken,
    ) -> Result<Vec<PreparedReplicationStream>, Error> {
        // Synchronize tables from publication.
        self.sync_tables(false, source).await?;

        // Create replication slots if we haven't already.
        if self.slots.is_empty() {
            Box::pin(self.create_slots(source, cancel)).await?;
        }

        for (number, _) in source.shards().iter().enumerate() {
            if !self.slots.contains_key(&number) {
                return Err(Error::NoReplicationSlot(number));
            }
        }

        let mut streams = Vec::with_capacity(source.shards().len());
        for (number, _) in source.shards().iter().enumerate() {
            // Use table offsets from data sync
            // or from loading them above.
            let tables = self.tables.remove(&number).unwrap_or_default();
            // Take ownership of the slot for replication.
            let slot = self.slots.remove(&number).expect("slot was validated");
            streams.push(PreparedReplicationStream {
                source_shard: number,
                slot,
                tables,
            });
        }

        Ok(streams)
    }

    pub(crate) fn post_data_sync(&mut self, tables: HashMap<usize, Vec<Table>>) {
        self.tables = tables;
    }

    /// Drop the replication slots created during data sync.
    ///
    /// Keep each slot registered until its cleanup attempt finishes, so aborting
    /// this future leaves unfinished slots available for retry. Every slot is
    /// attempted even if one fails; the first error is returned.
    pub(crate) async fn cleanup(&mut self) -> Result<(), Error> {
        let mut error = None;
        while let Some((&shard, slot)) = self.slots.iter_mut().next() {
            if let Err(err) = slot.drop_slot().await {
                error.get_or_insert(err);
            }
            self.slots.remove(&shard);
        }

        error.map_or(Ok(()), Err)
    }
}

#[derive(Debug)]
pub(crate) struct PreparedReplicationStream {
    pub(crate) source_shard: usize,
    pub(crate) slot: ReplicationSlot,
    pub(crate) tables: Vec<Table>,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::backend::replication::logical::subscriber::stream::StreamSubscriber;
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
        let mut stream = StreamSubscriber::new(&cluster, vec![]);
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
        let mut stream = StreamSubscriber::new(&cluster, vec![]);
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
