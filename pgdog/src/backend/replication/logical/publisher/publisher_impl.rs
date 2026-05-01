use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use pgdog_config::QueryParserEngine;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::{select, spawn, time::interval};
use tracing::{debug, info, warn};

use super::super::{publisher::Table, Error, TableValidationErrors};
use super::ReplicationSlot;

use crate::backend::replication::logical::subscriber::stream::StreamSubscriber;
use crate::backend::replication::publisher::progress::Progress;
use crate::backend::replication::publisher::Lsn;
use crate::backend::replication::{
    logical::publisher::ReplicationData, publisher::ParallelSyncManager,
};
use crate::backend::{pool::Request, Cluster};
use crate::config::Role;
use crate::net::replication::ReplicationMeta;

fn merge_table_lsns(
    tables: Vec<Table>,
    existing_lsns: Option<&HashMap<(String, String), Lsn>>,
) -> Vec<Table> {
    tables
        .into_iter()
        .map(|mut table| {
            if let Some(lsn) = existing_lsns.and_then(|tables| tables.get(&table.key())) {
                table.lsn = *lsn;
            }
            table
        })
        .collect()
}

#[derive(Debug, Default)]
pub struct Publisher {
    /// Name of the publication.
    publication: String,
    /// Shard -> Tables mapping.
    tables: HashMap<usize, Vec<Table>>,
    /// Replication slots.
    slots: HashMap<usize, ReplicationSlot>,
    /// Query parser engine.
    query_parser_engine: QueryParserEngine,
    /// Replication lag.
    replication_lag: Arc<Mutex<HashMap<usize, i64>>>,
    /// Last transaction.
    last_transaction: Arc<Mutex<Option<Instant>>>,
    /// Stop signal.
    stop: Arc<Notify>,
    /// Slot name.
    slot_name: String,
}

impl Publisher {
    pub fn new(
        publication: &str,
        query_parser_engine: QueryParserEngine,
        slot_name: String,
    ) -> Self {
        Self {
            publication: publication.to_string(),
            tables: HashMap::new(),
            slots: HashMap::new(),
            query_parser_engine,
            replication_lag: Arc::new(Mutex::new(HashMap::new())),
            stop: Arc::new(Notify::new()),
            last_transaction: Arc::new(Mutex::new(None)),
            slot_name,
        }
    }

    pub fn replication_slot(&self) -> &str {
        &self.slot_name
    }

    fn distribute_omnisharded_tables(
        &mut self,
        omnisharded: HashMap<(String, String), Table>,
        source: &Cluster,
    ) {
        let shard_count = source.shards().len();
        // Downstream paths (e.g. Publisher::replicate) iterate every shard and
        // require a (possibly empty) entry in `self.tables` for each one.
        for number in 0..shard_count {
            self.tables.entry(number).or_default();
        }
        for (shard_index, table) in omnisharded.into_values().enumerate() {
            let shard = shard_index % shard_count;
            if let Some(tables) = self.tables.get_mut(&shard) {
                tables.push(table);
            }
        }
    }

    /// Synchronize tables for all shards.
    pub async fn sync_tables(
        &mut self,
        data_sync: bool,
        source: &Cluster,
        dest: &Cluster,
    ) -> Result<(), Error> {
        let sharding_tables = dest.sharding_schema().tables;
        let existing_lsns: HashMap<usize, HashMap<(String, String), Lsn>> = self
            .tables
            .iter()
            .map(|(shard, tables)| {
                (
                    *shard,
                    tables
                        .iter()
                        .map(|table| (table.key(), table.lsn))
                        .collect::<HashMap<_, _>>(),
                )
            })
            .collect();

        // Omnisharded tables are split evenly between shards
        // during copy to avoid duplicate key errors.
        let mut omnisharded = HashMap::new();

        for (number, shard) in source.shards().iter().enumerate() {
            // Load tables from publication.
            let mut primary = shard.primary(&Request::default()).await?;
            let tables =
                Table::load(&self.publication, &mut primary, self.query_parser_engine).await?;

            // For data sync, split omni tables evenly between shards.
            if data_sync {
                for table in tables {
                    let omni = !table.is_sharded(&sharding_tables);
                    if omni {
                        omnisharded.insert(table.key(), table);
                    } else {
                        let entry = self.tables.entry(number).or_insert(vec![]);
                        entry.push(table);
                    }
                }
            } else {
                // For replication, process changes from all shards.
                let tables = merge_table_lsns(tables, existing_lsns.get(&number));
                self.tables.insert(number, tables);
            }
        }

        // Distribute omni tables roughly equally between all shards.
        self.distribute_omnisharded_tables(omnisharded, source);

        Ok(())
    }

    /// Create permanent slots for each shard.
    /// This uses a dedicated connection.
    ///
    /// N.B.: These are not synchronized across multiple shards.
    /// If you're doing a cross-shard transaction, parts of it can be lost.
    ///
    /// TODO: Add support for 2-phase commit.
    async fn create_slots(&mut self, source: &Cluster) -> Result<(), Error> {
        for (number, shard) in source.shards().iter().enumerate() {
            let addr = shard.primary(&Request::default()).await?.addr().clone();

            let mut slot = ReplicationSlot::replication(
                &self.publication,
                &addr,
                Some(self.slot_name.clone()),
                number,
            );
            slot.create_slot().await?;

            self.slots.insert(number, slot);
        }

        Ok(())
    }

    /// Replicate and fan-out data from a shard to N shards.
    ///
    /// This uses a dedicated replication slot which will survive crashes and reboots.
    /// N.B.: The slot needs to be manually dropped!
    pub async fn replicate(&mut self, source: &Cluster, dest: &Cluster) -> Result<Waiter, Error> {
        // Replicate shards in parallel.
        let mut streams = vec![];

        // Synchronize tables from publication.
        self.sync_tables(false, source, dest).await?;

        // Create replication slots if we haven't already.
        if self.slots.is_empty() {
            self.create_slots(source).await?;
        }

        for (number, _) in source.shards().iter().enumerate() {
            // Use table offsets from data sync
            // or from loading them above.
            let tables = self
                .tables
                .get(&number)
                .ok_or(Error::NoReplicationTables(number))?;
            // Handles the logical replication stream messages.
            let mut stream = StreamSubscriber::new(dest, tables);

            // Take ownership of the slot for replication.
            let mut slot = self
                .slots
                .remove(&number)
                .ok_or(Error::NoReplicationSlot(number))?;
            stream.set_current_lsn(slot.lsn().lsn);

            let mut check_lag = interval(Duration::from_secs(1));
            let replication_lag = self.replication_lag.clone();
            let stop = self.stop.clone();
            let last_transaction = self.last_transaction.clone();

            let source_cluster = source.clone();
            let dest = dest.clone();

            // Replicate in parallel.
            let handle = spawn(async move {
                slot.start_replication().await?;
                let progress = Progress::new_stream();

                loop {
                    select! {
                        _ = stop.notified() => {
                            slot.stop_replication().await?;
                        }

                        // This is cancellation-safe.
                        replication_data = slot.replicate(Duration::MAX) => {
                            let replication_data = replication_data?;

                            match replication_data {
                                Some(ReplicationData::CopyData(data)) => {
                                    let lsn = if let Some(ReplicationMeta::KeepAlive(ka)) =
                                        data.replication_meta()
                                    {
                                        if ka.reply() {
                                            slot.status_update(stream.status_update()).await?;
                                        }
                                        debug!(
                                            "origin at lsn {} [{}]",
                                            Lsn::from_i64(ka.wal_end),
                                            slot.server()?.addr()
                                        );
                                        ka.wal_end
                                    } else {
                                        if let Some(status_update) = stream.handle(data).await? {
                                            slot.status_update(status_update).await?;
                                            *last_transaction.lock() = Some(Instant::now());
                                        }
                                        stream.lsn()
                                    };
                                    progress.update(stream.bytes_sharded(), lsn);
                                }
                                Some(ReplicationData::CopyDone) => (),
                                None => {
                                    slot.drop_slot().await?;
                                    break;
                                }
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

        Ok(Waiter {
            streams,
            stop: self.stop.clone(),
        })
    }

    /// Request the publisher to stop replication.
    pub fn request_stop(&self) {
        self.stop.notify_one();
    }

    /// Get current replication lag.
    pub fn replication_lag(&self) -> HashMap<usize, i64> {
        self.replication_lag.lock().clone()
    }

    /// Get how long ago last transaction was committed.
    pub fn last_transaction(&self) -> Option<Duration> {
        (*self.last_transaction.lock()).map(|last| last.elapsed())
    }

    /// Sync data from all tables in a publication from one shard to N shards,
    /// re-sharding the cluster in the process.
    ///
    /// TODO: Parallelize shard syncs.
    pub async fn data_sync(&mut self, source: &Cluster, dest: &Cluster) -> Result<(), Error> {
        // Fetch schema and column metadata first — valid() depends on it.
        self.sync_tables(true, source, dest).await?;

        // Validate all tables support replication before committing to
        // what can be a multi-hour copy.  A table with no primary key or
        // unique replica-identity index cannot be replicated correctly.
        let mut validation_errors: Vec<_> = self
            .tables
            .values()
            .flat_map(|t| t.iter())
            .filter_map(|t| t.valid().err())
            .collect();

        if !validation_errors.is_empty() {
            validation_errors.sort_by_key(|e| e.table.name.clone());

            return Err(Error::TableValidation(TableValidationErrors(
                validation_errors,
            )));
        }

        // Create replication slots only after validation passes — a slot
        // created before valid() would be orphaned on a NoIdentityColumns error.
        self.create_slots(source).await?;

        let mut handles = vec![];

        for (number, shard) in source.shards().iter().enumerate() {
            let tables = self
                .tables
                .get(&number)
                .ok_or(Error::NoReplicationTables(number))?
                .clone();

            info!(
                "table sync starting for {} tables, shard={}",
                tables.len(),
                number
            );

            let include_primary = !shard.has_replicas();
            let resharding_only = shard
                .pools()
                .into_iter()
                .filter(|pool| pool.config().resharding_only)
                .collect::<Vec<_>>();
            let replicas = if resharding_only.is_empty() {
                shard
                    .pools_with_roles()
                    .into_iter()
                    .filter(|(r, _)| match *r {
                        Role::Replica => true,
                        Role::Primary => include_primary,
                        Role::Auto => false,
                    })
                    .map(|(_, p)| p)
                    .collect::<Vec<_>>()
            } else {
                resharding_only
            };

            let dest = dest.clone();
            handles.push(spawn(async move {
                let manager = ParallelSyncManager::new(tables, replicas, dest)?;
                let tables = manager.run().await?;

                Ok::<Vec<Table>, Error>(tables)
            }));
        }

        for (number, handle) in handles.into_iter().enumerate() {
            let tables = handle.await??;

            info!(
                "table sync for {} tables complete [{}, shard: {}]",
                tables.len(),
                source.name(),
                number,
            );

            // Update table LSN positions.
            self.tables.insert(number, tables);
        }

        Ok(())
    }

    /// Cleanup after replication.
    pub async fn cleanup(&mut self) -> Result<(), Error> {
        for slot in self.slots.values_mut() {
            slot.drop_slot().await?;
        }

        Ok(())
    }
}

#[cfg(test)]
impl Publisher {
    pub fn set_replication_lag(&self, shard: usize, lag: i64) {
        self.replication_lag.lock().insert(shard, lag);
    }

    pub fn set_last_transaction(&self, instant: Option<Instant>) {
        *self.last_transaction.lock() = instant;
    }
}

#[derive(Debug)]
pub struct Waiter {
    streams: Vec<JoinHandle<Result<(), Error>>>,
    stop: Arc<Notify>,
}

impl Waiter {
    pub fn stop(&self) {
        self.stop.notify_one();
    }

    pub async fn wait(&mut self) -> Result<(), Error> {
        for stream in &mut self.streams {
            stream.await??;
        }

        Ok(())
    }
}

#[cfg(test)]
impl Waiter {
    pub fn new_test() -> Self {
        Self {
            streams: vec![],
            stop: Arc::new(Notify::new()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::backend::replication::logical::publisher::{
        PublicationTable, PublicationTableColumn, ReplicaIdentity,
    };
    use crate::backend::server::test::test_replication_server;
    use crate::config::config;

    fn make_table(schema: &str, name: &str, lsn: i64) -> Table {
        Table {
            publication: "test".to_string(),
            table: PublicationTable {
                schema: schema.to_string(),
                name: name.to_string(),
                attributes: String::new(),
                parent_schema: String::new(),
                parent_name: String::new(),
            },
            identity: ReplicaIdentity {
                oid: pgdog_postgres_types::Oid(1),
                identity: String::new(),
                kind: String::new(),
            },
            columns: vec![PublicationTableColumn {
                oid: 1,
                name: "tenant_id".to_string(),
                type_oid: pgdog_postgres_types::Oid(20),
                identity: true,
            }],
            lsn: Lsn::from_i64(lsn),
            query_parser_engine: QueryParserEngine::default(),
        }
    }

    #[test]
    fn merge_table_lsns_preserves_existing_offsets() {
        let existing = HashMap::from([(
            ("copy_data".to_string(), "users".to_string()),
            Lsn::from_i64(123),
        )]);

        let merged = merge_table_lsns(vec![make_table("copy_data", "users", 0)], Some(&existing));

        assert_eq!(merged[0].lsn, Lsn::from_i64(123));
    }

    #[test]
    fn merge_table_lsns_leaves_unknown_tables_unset() {
        let existing = HashMap::from([(
            ("copy_data".to_string(), "users".to_string()),
            Lsn::from_i64(123),
        )]);

        let merged = merge_table_lsns(vec![make_table("copy_data", "orders", 0)], Some(&existing));

        assert_eq!(merged[0].lsn, Lsn::default());
    }

    #[test]
    fn distribute_omnisharded_tables_initializes_missing_shards() {
        let config = config();
        let cluster = Cluster::new_test(&config);
        let mut publisher = Publisher::new("test", QueryParserEngine::default(), "slot".into());
        let table = make_table("public", "omni_only", 0);

        publisher.distribute_omnisharded_tables(HashMap::from([(table.key(), table)]), &cluster);

        assert!(
            publisher.tables.contains_key(&0),
            "omni-only publications should initialize shard 0 even when no sharded tables exist"
        );
        assert!(
            publisher.tables.contains_key(&1),
            "data_sync iterates every shard and needs an entry for shard 1 even if it is empty"
        );
        assert_eq!(
            publisher.tables.values().map(Vec::len).sum::<usize>(),
            1,
            "the omnisharded table should still be assigned exactly once"
        );
    }

    /// Tables without a primary key or replica identity index must be rejected
    /// before the copy starts, not after. Validates that `data_sync` returns
    /// `TableValidation` carrying one entry per bad table and leaves no replication slots behind.
    #[tokio::test]
    async fn data_sync_rejects_no_pk_table_before_slots_created() {
        crate::logger();

        // Three tables with no replica identity — each would fail replication.
        let mut server = test_replication_server().await;
        for ddl in &[
            "CREATE TABLE IF NOT EXISTS publication_test_no_pk   (data TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS publication_test_no_pk_2 (payload JSONB)",
            "CREATE TABLE IF NOT EXISTS publication_test_no_pk_3 (ts TIMESTAMPTZ NOT NULL DEFAULT now(), value FLOAT8)",
            "DROP PUBLICATION IF EXISTS publication_no_pk_validation",
            "CREATE PUBLICATION publication_no_pk_validation FOR TABLE publication_test_no_pk, publication_test_no_pk_2, publication_test_no_pk_3",
        ] {
            server.execute(*ddl).await.unwrap();
        }

        // Real cluster so metadata is fetched from Postgres, not synthetic.
        let source = Cluster::new_test(&config());
        source.launch();
        let dest = Cluster::new_test(&config());

        let mut publisher = Publisher::new(
            "publication_no_pk_validation",
            QueryParserEngine::default(),
            "sync_test_slot".into(),
        );

        // Validation must fire before the copy begins.
        let result = publisher.data_sync(&source, &dest).await;

        let err = result.expect_err("data_sync must fail for a publication with no-pk tables");

        // Errors are sorted by table name — assert the exact rendered output.
        assert_eq!(
            err.to_string(),
            "Table validation failed:\n\
            \ttable \"pgdog\".\"publication_test_no_pk\": has no replica identity columns\n\
            \ttable \"pgdog\".\"publication_test_no_pk_2\": has no replica identity columns\n\
            \ttable \"pgdog\".\"publication_test_no_pk_3\": has no replica identity columns",
        );

        assert!(
            publisher.slots.is_empty(),
            "no replication slots should be created when valid() fails",
        );

        source.shutdown();
        for ddl in &[
            "DROP PUBLICATION IF EXISTS publication_no_pk_validation",
            "DROP TABLE IF EXISTS publication_test_no_pk_3",
            "DROP TABLE IF EXISTS publication_test_no_pk_2",
            "DROP TABLE IF EXISTS publication_test_no_pk",
        ] {
            server.execute(*ddl).await.unwrap();
        }
    }

    /// `REPLICA IDENTITY NOTHING` must be rejected at `data_sync` time,
    /// before any replication slot is created. This test executes against
    /// a real Postgres instance so it validates the full metadata-fetch + valid() path.
    #[tokio::test]
    async fn data_sync_rejects_replica_identity_nothing() {
        crate::logger();

        let mut server = test_replication_server().await;
        for ddl in &[
            "CREATE TABLE IF NOT EXISTS pub_test_nothing (data TEXT NOT NULL)",
            "ALTER TABLE pub_test_nothing REPLICA IDENTITY NOTHING",
            "DROP PUBLICATION IF EXISTS pub_full_identity_nothing_test",
            "CREATE PUBLICATION pub_full_identity_nothing_test FOR TABLE pub_test_nothing",
        ] {
            server.execute(*ddl).await.unwrap();
        }

        let source = Cluster::new_test(&config());
        source.launch();
        let dest = Cluster::new_test(&config());

        let mut publisher = Publisher::new(
            "pub_full_identity_nothing_test",
            QueryParserEngine::default(),
            "pub_full_identity_nothing_slot".into(),
        );

        let result = publisher.data_sync(&source, &dest).await;

        let err = result.expect_err("data_sync must fail for REPLICA IDENTITY NOTHING table");
        assert!(
            err.to_string().contains("REPLICA IDENTITY NOTHING"),
            "expected NOTHING in error message, got: {err}"
        );
        assert!(
            publisher.slots.is_empty(),
            "no replication slot must be created when NOTHING table is present"
        );

        source.shutdown();
        for ddl in &[
            "DROP PUBLICATION IF EXISTS pub_full_identity_nothing_test",
            "DROP TABLE IF EXISTS pub_test_nothing",
        ] {
            server.execute(*ddl).await.unwrap();
        }
    }
}
