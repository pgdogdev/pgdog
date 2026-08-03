//! Create and drop publications for ADD SHARD.
//!
//! Unlike resharding, which requires an operator-created publication,
//! ADD SHARD can create its own: it only needs the omnisharded tables,
//! which are enumerated from the config.

use tracing::{info, warn};

use super::super::Error;
use super::queries::PublicationTable;
use crate::backend::{Cluster, Server, pool::Request};
use crate::util::escape_identifier;

/// Create a publication for the given tables on a shard's primary.
///
/// If a publication with this name already exists, verify it covers
/// exactly the requested tables and reuse it; a mismatch is an error,
/// since copying and streaming trust the publication's table list.
pub(crate) async fn create_publication(
    cluster: &Cluster,
    shard: usize,
    name: &str,
    tables: &[String],
) -> Result<(), Error> {
    let mut server = cluster
        .shards()
        .get(shard)
        .ok_or(crate::backend::pool::Error::NoShard(shard))?
        .primary(&Request::default())
        .await?;
    create_publication_on(&mut server, name, tables).await
}

/// Like [`create_publication`], on an already-connected server.
pub(crate) async fn create_publication_on(
    server: &mut Server,
    name: &str,
    tables: &[String],
) -> Result<(), Error> {
    if tables.is_empty() {
        return Err(Error::NoTables);
    }

    let table_list = tables
        .iter()
        .map(|table| format!("\"{}\"", escape_identifier(table)))
        .collect::<Vec<_>>()
        .join(", ");
    let create = format!(
        "CREATE PUBLICATION \"{}\" FOR TABLE {}",
        escape_identifier(name),
        table_list
    );

    match server.execute_checked(create.as_str()).await {
        Ok(_) => {
            info!(
                "created publication \"{}\" for {} table(s)",
                name,
                tables.len()
            );
            Ok(())
        }
        // 42710: publication already exists. Verify it matches.
        Err(crate::backend::Error::ExecutionError(response)) if response.code == "42710" => {
            let mut existing = PublicationTable::load(name, server)
                .await?
                .into_iter()
                .map(|table| table.name)
                .collect::<Vec<_>>();
            existing.sort();
            let mut requested = tables.to_vec();
            requested.sort();

            if existing == requested {
                info!("reusing existing publication \"{}\"", name);
                Ok(())
            } else {
                Err(Error::PublicationMismatch(name.to_owned()))
            }
        }
        Err(err) => Err(err.into()),
    }
}

/// Drop a publication on a shard's primary, best effort: failures are
/// logged, not returned, since this runs on cleanup paths.
pub(crate) async fn drop_publication(cluster: &Cluster, shard: usize, name: &str) {
    let result: Result<(), Error> = async {
        let mut server = cluster
            .shards()
            .get(shard)
            .ok_or(crate::backend::pool::Error::NoShard(shard))?
            .primary(&Request::default())
            .await?;
        drop_publication_on(&mut server, name).await
    }
    .await;

    if let Err(err) = result {
        warn!("could not drop publication \"{}\": {}", name, err);
    }
}

/// Like [`drop_publication`], on an already-connected server; errors
/// are returned rather than logged.
pub(crate) async fn drop_publication_on(server: &mut Server, name: &str) -> Result<(), Error> {
    server
        .execute_checked(
            format!("DROP PUBLICATION IF EXISTS \"{}\"", escape_identifier(name)).as_str(),
        )
        .await?;
    info!("dropped publication \"{}\"", name);
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::backend::server::test::test_server;

    #[tokio::test]
    async fn test_create_verify_drop_publication() {
        let mut server = test_server().await;
        let name = "__pgdog_test_add_shard_pub";

        server
            .execute_checked("CREATE TABLE IF NOT EXISTS pub_omni_a (id BIGINT PRIMARY KEY)")
            .await
            .unwrap();
        server
            .execute_checked("CREATE TABLE IF NOT EXISTS pub_omni_b (id BIGINT PRIMARY KEY)")
            .await
            .unwrap();
        drop_publication_on(&mut server, name).await.unwrap();

        let tables = vec!["pub_omni_a".to_string(), "pub_omni_b".to_string()];

        // Fresh create.
        create_publication_on(&mut server, name, &tables)
            .await
            .unwrap();

        // Identical table set: reused, not an error.
        create_publication_on(&mut server, name, &tables)
            .await
            .unwrap();

        // Different table set: refused.
        let mismatched = vec!["pub_omni_a".to_string()];
        let err = create_publication_on(&mut server, name, &mismatched)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::PublicationMismatch(_)));

        // Empty table list: refused.
        let err = create_publication_on(&mut server, "__pgdog_test_empty", &[])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NoTables));

        // Cleanup.
        drop_publication_on(&mut server, name).await.unwrap();
        server
            .execute_checked("DROP TABLE IF EXISTS pub_omni_a, pub_omni_b")
            .await
            .unwrap();
    }
}
