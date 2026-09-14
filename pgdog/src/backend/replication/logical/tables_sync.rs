use std::collections::{HashMap, HashSet};

use super::Error;
use super::publisher::Table;
use crate::backend::pool::Request;
use crate::backend::{Cluster, ShardedTables};

/// Fetch the info about tables on shard and distribute the set of tables
/// between the shards.
///
/// # Invariants
///
/// - the omni-sharded tables should be present on all the shards and should have
///   identical content, this is due the fact that the omni-sharded table is
///   copied/replicated only from single shard (random) and only this shard's
///   data will be used to update the destination shards.
pub(crate) async fn tables_sync(
    source: &Cluster,
    // this should be [`ShardedTables`] configuration from the source cluster
    sharded_tables: &ShardedTables,
    publication: &str,
) -> Result<HashMap<usize, Vec<Table>>, Error> {
    let mut result: HashMap<usize, Vec<Table>> = HashMap::new();

    let shards_count = source.shards().len();
    let mut omnisharded = HashSet::new();

    for shard in source.shards() {
        let mut primary = shard.primary(&Request::default()).await?;
        let tables = Table::load(publication, &mut primary).await?;

        for table in tables {
            let shard_index = if table.is_sharded(sharded_tables) {
                // if table is sharded on source, then we push this table to this shard
                // so it'll be copied/replicated from this shard changes
                shard.number()
            } else {
                // if the table is omnisharded, then check if we already saw it
                // and if not, move it to only one shard
                if !omnisharded.insert(table.key()) {
                    continue;
                }
                (omnisharded.len() - 1) % shards_count
            };

            result.entry(shard_index).or_default().push(table);
        }
    }

    if result.is_empty() {
        return Err(Error::EmptyPublication(publication.to_owned()));
    }

    Ok(result)
}

#[cfg(test)]
mod test {
    use crate::backend::replication::logical::publisher::test::{
        PublicationTest, setup_publication_tables,
    };
    use crate::config::{DataType, Hasher, config};
    use crate::frontend::router::sharding::ShardedTable;

    use super::*;

    fn sharded_tables(names: &[&str]) -> ShardedTables {
        names
            .iter()
            .map(|name| ShardedTable {
                database: "pgdog".into(),
                name: Some((*name).into()),
                column: "id".into(),
                primary: true,
                data_type: DataType::Bigint,
                centroid_probes: 1,
                hasher: Hasher::Postgres,
                ..Default::default()
            })
            .collect::<Vec<_>>()
            .as_slice()
            .into()
    }

    async fn distribute(publication: &PublicationTest, sharded: &[&str]) -> Vec<Vec<String>> {
        let source = Cluster::new_test(&config());
        source.launch();

        let shards = source.shards().len();
        let distribution = tables_sync(&source, &sharded_tables(sharded), &publication.publication)
            .await
            .unwrap();

        source.shutdown();

        (0..shards)
            .map(|shard| {
                let mut names = distribution
                    .get(&shard)
                    .map(|tables| {
                        tables
                            .iter()
                            .map(|table| table.table.name.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                names.sort();
                names
            })
            .collect()
    }

    #[tokio::test]
    async fn test_sharded_tables_go_to_every_shard() {
        crate::logger();

        let tables = ["tables_sync_sharded_one", "tables_sync_sharded_two"];
        let mut publication = setup_publication_tables("tables_sync_sharded_pub", &tables).await;

        let distribution = distribute(&publication, &tables).await;

        assert_eq!(distribution.len(), 2);
        assert_eq!(distribution[0], tables);
        assert_eq!(distribution[1], tables);

        publication.cleanup().await;
    }

    #[tokio::test]
    async fn test_omnisharded_tables_are_distributed() {
        crate::logger();

        let tables = [
            "tables_sync_omni_a",
            "tables_sync_omni_b",
            "tables_sync_omni_c",
            "tables_sync_omni_d",
        ];
        let mut publication = setup_publication_tables("tables_sync_omni_pub", &tables).await;

        let distribution = distribute(&publication, &[]).await;

        assert_eq!(distribution.len(), 2);
        assert_eq!(
            distribution[0],
            ["tables_sync_omni_a", "tables_sync_omni_c"]
        );
        assert_eq!(
            distribution[1],
            ["tables_sync_omni_b", "tables_sync_omni_d"]
        );

        publication.cleanup().await;
    }

    #[tokio::test]
    async fn test_mixed_tables_distribution() {
        crate::logger();

        let sharded = "tables_sync_mixed_sharded";
        let omni = [
            "tables_sync_mixed_omni_a",
            "tables_sync_mixed_omni_b",
            "tables_sync_mixed_omni_c",
        ];
        let tables = [omni[0], omni[1], omni[2], sharded];
        let mut publication = setup_publication_tables("tables_sync_mixed_pub", &tables).await;

        let distribution = distribute(&publication, &[sharded]).await;

        assert_eq!(distribution.len(), 2);
        assert_eq!(distribution[0], [omni[0], omni[2], sharded]);
        assert_eq!(distribution[1], [omni[1], sharded]);

        publication.cleanup().await;
    }
}
