use super::super::{Error, Table};
use crate::{backend::Cluster, config::Role};

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

    /// Sync data from a publication into a different cluster.
    ///
    /// Data is re-sharded automatically.
    pub async fn data_sync(&self, dest: &Cluster) -> Result<(), Error> {
        for shard in self.cluster.shards() {
            let mut primary = shard
                .pools_with_roles()
                .iter()
                .filter(|(r, _)| r == &Role::Primary)
                .next()
                .ok_or(Error::NoPrimary)?
                .1
                .standalone()
                .await?;

            let tables = Table::load(&self.publication, &mut primary).await?;

            for mut table in tables {
                table.data_sync(primary.addr(), dest).await?;
            }
        }

        Ok(())
    }
}
