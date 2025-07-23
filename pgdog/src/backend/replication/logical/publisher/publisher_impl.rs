use std::time::Duration;

use tokio::{select, spawn};
use tracing::error;

use super::super::{publisher::Table, Error};
use super::ReplicationSlot;

use crate::backend::replication::logical::publisher::ReplicationData;
use crate::backend::replication::logical::subscriber::stream::StreamSubscriber;
use crate::backend::{pool::Request, Cluster};
use crate::net::replication::{ReplicationMeta, StatusUpdate};

#[derive(Debug)]
pub struct Publisher {
    cluster: Cluster,
    publication: String,
}

impl Publisher {
    pub fn new(cluster: &Cluster, publication: &str) -> Self {
        Self {
            cluster: cluster.clone(),
            publication: publication.to_string(),
        }
    }

    /// Replicate and fan-out data from a shard to N shards.
    ///
    /// This uses a dedicated replication slot which will survive crashes and reboots.
    /// N.B.: The slot needs to be manually dropped!
    pub async fn replicate(&self, dest: &Cluster) -> Result<(), Error> {
        // Replicate shards in parallel.
        let mut streams = vec![];

        for shard in self.cluster.shards() {
            // Load tables from publication.
            let mut primary = shard.primary(&Request::default()).await?;
            let tables = Table::load(&self.publication, &mut primary).await?;

            // Handles the logical replication stream messages.
            let mut stream = StreamSubscriber::new(dest, &tables);

            // Create permanent slot.
            // This uses a dedicated connection.
            //
            // N.B.: These are not synchronized across multiple shards.
            // If you're doing a cross-shard transaction, parts of it can be lost.
            //
            // TODO: Add support for 2-phase commit.
            let mut slot = ReplicationSlot::replication(&self.publication, primary.addr());
            slot.create_slot().await?;

            // Replicate in parallel.
            let handle = spawn(async move {
                slot.start_replication().await?;

                loop {
                    select! {
                        replication_data = slot.replicate(Duration::MAX) => {
                            match replication_data {
                                Ok(Some(ReplicationData::CopyData(data))) => {
                                    // We process one message at a time and either succeed or fail.
                                    // Since the individual stream is synchronized, if we receive a keep-alive,
                                    // we already flushed whatever came beforehand.
                                    if let Some(ReplicationMeta::KeepAlive(keep_alive)) = data.replication_meta() {
                                        slot.status_update(StatusUpdate::from(keep_alive)).await?;
                                    } else {
                                        stream.handle(data).await?;
                                    }
                                }
                                Ok(Some(ReplicationData::CopyDone)) => (),
                                Ok(None) => {
                                    slot.drop_slot().await?;
                                    break;
                                }
                                Err(err) => {
                                    return Err(err);
                                }
                            }
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
    /// TODO: This is currently NOT synchronized with a replication slot.
    pub async fn data_sync(&self, dest: &Cluster) -> Result<(), Error> {
        for shard in self.cluster.shards() {
            let mut primary = shard.primary(&Request::default()).await?;
            let tables = Table::load(&self.publication, &mut primary).await?;

            // Do one table a table. Parallelizing this doesn't really help,
            // since Postgres is bottle-necked by writes, not reads.
            for mut table in tables {
                table.data_sync(primary.addr(), dest).await?;
            }
        }

        Ok(())
    }
}
