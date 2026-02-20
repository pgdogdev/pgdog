use std::collections::HashMap;
use std::time::Duration;

use pgdog_config::QueryParserEngine;
use tokio::{select, spawn, time::interval};
use tracing::{debug, error, info};

use super::super::{publisher::Table, Error};
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

#[derive(Debug)]
pub struct Publisher {
    /// Destination cluster.
    cluster: Cluster,
    /// Name of the publication.
    publication: String,
    /// Shard -> Tables mapping.
    tables: HashMap<usize, Vec<Table>>,
    /// Replication slots.
    slots: HashMap<usize, ReplicationSlot>,
    /// Query parser engine.
    query_parser_engine: QueryParserEngine,
}

impl Publisher {
    pub fn new(
        cluster: &Cluster,
        publication: &str,
        query_parser_engine: QueryParserEngine,
    ) -> Self {
        Self {
            cluster: cluster.clone(),
            publication: publication.to_string(),
            tables: HashMap::new(),
            slots: HashMap::new(),
            query_parser_engine,
        }
    }

    /// Synchronize tables for all shards.
    pub async fn sync_tables(&mut self) -> Result<(), Error> {
        for (number, shard) in self.cluster.shards().iter().enumerate() {
            // Load tables from publication.
            let mut primary = shard.primary(&Request::default()).await?;
            let tables =
                Table::load(&self.publication, &mut primary, self.query_parser_engine).await?;

            self.tables.insert(number, tables);
        }

        Ok(())
    }

    /// Create permanent slots for each shard.
    /// This uses a dedicated connection.
    ///
    /// N.B.: These are not synchronized across multiple shards.
    /// If you're doing a cross-shard transaction, parts of it can be lost.
    ///
    /// TODO: Add support for 2-phase commit.
    async fn create_slots(&mut self, slot_name: Option<String>) -> Result<(), Error> {
        for (number, shard) in self.cluster.shards().iter().enumerate() {
            let addr = shard.primary(&Request::default()).await?.addr().clone();

            let mut slot =
                ReplicationSlot::replication(&self.publication, &addr, slot_name.clone());
            slot.create_slot().await?;

            self.slots.insert(number, slot);
        }

        Ok(())
    }

    /// Replicate and fan-out data from a shard to N shards.
    ///
    /// This uses a dedicated replication slot which will survive crashes and reboots.
    /// N.B.: The slot needs to be manually dropped!
    pub async fn replicate(
        &mut self,
        dest: &Cluster,
        slot_name: Option<String>,
    ) -> Result<(), Error> {
        // Replicate shards in parallel.
        let mut streams = vec![];

        // Synchronize tables from publication.
        if self.tables.is_empty() {
            self.sync_tables().await?;
        }

        // Create replication slots if we haven't already.
        if self.slots.is_empty() {
            self.create_slots(slot_name).await?;
        }

        for (number, _) in self.cluster.shards().iter().enumerate() {
            // Use table offsets from data sync
            // or from loading them above.
            let tables = self
                .tables
                .get(&number)
                .ok_or(Error::NoReplicationTables(number))?;
            // Handles the logical replication stream messages.
            let mut stream = StreamSubscriber::new(dest, tables, self.query_parser_engine);

            // Take ownership of the slot for replication.
            let mut slot = self
                .slots
                .remove(&number)
                .ok_or(Error::NoReplicationSlot(number))?;
            stream.set_current_lsn(slot.lsn().lsn);

            let mut check_lag = interval(Duration::from_secs(1));

            // Replicate in parallel.
            let handle = spawn(async move {
                slot.start_replication().await?;
                let progress = Progress::new_stream();

                loop {
                    select! {
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

                            info!(
                                "replication lag at {} bytes [{}]",
                                lag,
                                slot.server()?.addr()
                            );
                        }
                    }
                }

                Ok::<(), Error>(())
            });

            streams.push(handle);
        }

        for (shard, stream) in streams.into_iter().enumerate() {
            if let Err(err) = stream.await.unwrap() {
                error!("error replicating from shard {}: {}", shard, err);
                return Err(err);
            }
        }

        Ok(())
    }

    /// Sync data from all tables in a publication from one shard to N shards,
    /// re-sharding the cluster in the process.
    ///
    /// TODO: Parallelize shard syncs.
    pub async fn data_sync(
        &mut self,
        dest: &Cluster,
        replicate: bool,
        slot_name: Option<String>,
    ) -> Result<(), Error> {
        // Create replication slots.
        self.create_slots(slot_name.clone()).await?;

        for (number, shard) in self.cluster.shards().iter().enumerate() {
            let mut primary = shard.primary(&Request::default()).await?;
            let tables =
                Table::load(&self.publication, &mut primary, self.query_parser_engine).await?;

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

            let manager = ParallelSyncManager::new(tables, replicas, dest)?;
            let tables = manager.run().await?;

            info!(
                "table sync for {} tables complete [{}, shard: {}]",
                tables.len(),
                self.cluster.name(),
                number,
            );

            // Update table LSN positions.
            self.tables.insert(number, tables);
        }

        if replicate {
            // Replicate changes.
            self.replicate(dest, slot_name).await?;
        }

        Ok(())
    }
}
