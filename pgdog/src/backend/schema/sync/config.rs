//! Shard configuration sync.

use tracing::info;

use crate::backend::pool::Request;
use crate::backend::Cluster;
use crate::backend::Error;
use crate::backend::Pool;
use crate::backend::Schema;

pub struct ShardConfig {
    shard: usize,
    shards: usize,
    pool: Pool,
}

impl ShardConfig {
    /// Sync schema and set shard config.
    pub async fn sync(&self) -> Result<(), Error> {
        let mut conn = self.pool.get(&Request::default()).await?;

        Schema::setup(&mut conn).await?;

        conn.execute("BEGIN").await?;
        conn.execute("DELETE FROM pgdog.config").await?;
        conn.execute(format!(
            "INSERT INTO pgdog.config (shard, shards) VALUES ({}, {})",
            self.shard, self.shards
        ))
        .await?;
        conn.execute("COMMIT").await?;

        Ok(())
    }

    /// Sync all shards.
    pub async fn sync_all(cluster: &Cluster) -> Result<(), Error> {
        let shards = cluster.shards().len();

        info!("setting up schema on {} shards", shards);

        let shards: Vec<_> = cluster
            .shards()
            .iter()
            .filter(|s| s.has_primary())
            .map(|s| s.pools().first().unwrap().clone())
            .enumerate()
            .map(|(shard, pool)| ShardConfig {
                pool,
                shard,
                shards,
            })
            .collect();

        for shard in &shards {
            shard.sync().await?;
        }

        info!("schema setup complete for {} shards", shards.len());

        Ok(())
    }
}
