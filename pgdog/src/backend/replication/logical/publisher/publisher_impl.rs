use tokio::spawn;
use tracing::error;

use super::super::{Error, Table};
use super::ReplicationSlot;

use crate::backend::replication::logical::subscriber::stream::StreamSubscriber;
use crate::backend::replication::logical::ReplicationData;
use crate::{
    backend::{pool::Request, Cluster},
    config::Role,
};

#[derive(Debug)]
pub struct Publisher {
    cluster: Cluster,
    publication: String,
    slot: Option<ReplicationSlot>,
}

impl Publisher {
    pub fn new(cluster: &Cluster, publication: &str) -> Self {
        Self {
            cluster: cluster.clone(),
            publication: publication.to_string(),
            slot: None,
        }
    }

    /// Replicate data from shard.
    pub async fn replicate(&self, dest: &Cluster) -> Result<(), Error> {
        let mut streams = vec![];
        for shard in self.cluster.shards() {
            let mut primary = shard.primary(&Request::default()).await?;
            let tables = Table::load(&self.publication, &mut primary).await?;
            let mut stream = StreamSubscriber::new(dest, &tables);

            let mut slot = ReplicationSlot::replication(&self.publication, primary.addr());
            slot.create_slot().await?;

            let handle = spawn(async move {
                loop {
                    match slot.replicate().await {
                        Ok(Some(ReplicationData::CopyData(data))) => {
                            if let Err(err) = stream.handle(data).await {
                                error!("replication error: {}", err);
                            }
                        }
                        Ok(Some(ReplicationData::CopyDone)) => (),
                        Ok(None) => {
                            if let Err(err) = slot.drop_slot().await {
                                error!("replication error while dropping slot: {}", err);
                            }
                            break;
                        }
                        Err(err) => {
                            error!("replication error: {}", err);
                        }
                    }
                }
            });
            streams.push(handle);
        }

        for stream in streams {
            let _ = stream.await;
        }

        Ok(())
    }

    /// Sync data from a publication into a different cluster.
    ///
    /// Data is re-sharded automatically.
    pub async fn data_sync(&self, dest: &Cluster) -> Result<(), Error> {
        for shard in self.cluster.shards() {
            let mut primary = shard.primary(&Request::default()).await?;
            let tables = Table::load(&self.publication, &mut primary).await?;

            for mut table in tables {
                table.data_sync(primary.addr(), dest).await?;
            }
        }

        Ok(())
    }
}
