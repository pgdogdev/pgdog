use std::collections::HashMap;
use std::time::Duration;

use tokio::select;
use tracing::{info, warn};

use pgdog_config::CopyFormat;

use crate::backend::pool::{Address, Request};
use crate::backend::{Cluster, ConnectReason, Server, ServerOptions};
use crate::net::prelude::Protocol;
use crate::net::replication::StatusUpdate;
use crate::net::{DataRow, Format};
use crate::util::escape_identifier;
use crate::util::sql::quote_literal;
use tokio_util::sync::CancellationToken;

use super::Error;
use super::ensure_validation;
use super::publisher::{Copy, ReplicationSlot, Table};
use super::subscriber::CopySubscriber;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CopyProgress {
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TableEstimate {
    pub(crate) rows: Option<u64>,
    pub(crate) bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DataSync<'a> {
    pub(crate) source: &'a Cluster,
    pub(crate) dest: &'a Cluster,
    pub(crate) format: CopyFormat,
}

impl DataSync<'_> {
    /// Copy one table from a source shard to the destination cluster.
    ///
    /// Returns the table with the slot's consistent-point LSN on success.
    pub(crate) async fn copy_table(
        &self,
        table: &Table,
        address: &Address,
        cancel: &CancellationToken,
        mut on_progress: impl FnMut(CopyProgress),
    ) -> Result<Table, Error> {
        let mut table = table.clone();

        info!(
            "data sync for \"{}\".\"{}\" started [{}]",
            table.table.schema, table.table.name, address
        );

        // Publisher uses COPY [...] TO STDOUT.
        // Subscriber uses COPY [...] FROM STDIN.
        let copy = Copy::new(&table, self.format);

        let mut copy_sub = CopySubscriber::new(copy.statement(), self.source, self.dest)?;
        copy_sub.connect().await?;

        let mut slot = ReplicationSlot::data_sync(&table.publication, address);
        slot.connect().await?;
        table.lsn = slot.create_slot().await?;

        // Reload table info just to be sure it's consistent.
        table.reload(slot.server()?).await?;

        copy.start(slot.server()?).await?;
        copy_sub.start_copy().await?;

        let mut copied = CopyProgress::default();

        while let Some(data_row) = copy.data(slot.server()?).await? {
            select! {
                _ = cancel.cancelled() =>  {
                    warn!("aborting data sync for table {}", table.table);

                    return Err(Error::CopyAborted(table.table.clone()))
                },
                result = copy_sub.copy_data(data_row) => {
                    let (rows, bytes) = result?;
                    copied.rows += rows as u64;
                    copied.bytes += bytes as u64;
                    on_progress(copied);
                }
            }
        }

        copy_sub.copy_done().await?;

        copy_sub.disconnect().await?;

        slot.server()?.execute("COMMIT").await?;

        slot.start_replication().await?;
        slot.status_update(StatusUpdate::new_reply(table.lsn))
            .await?;
        slot.stop_replication().await?;

        // Drain slot. It is temporary and will be dropped when the connection closes.
        while slot.replicate(Duration::MAX).await?.is_some() {}

        info!(
            "data sync for \"{}\".\"{}\" finished at lsn {} [{}]",
            table.table.schema, table.table.name, table.lsn, address
        );

        Ok(table)
    }
}

/// Get the estimation of number of rows and bytes occupied by
/// the table
pub(crate) async fn estimate_table(
    table: &Table,
    address: &Address,
) -> Result<TableEstimate, Error> {
    let sql = format!(
        "SELECT c.reltuples::bigint, \
         pg_relation_size(c.oid) + CASE WHEN c.reltoastrelid <> 0 \
         THEN pg_total_relation_size(c.reltoastrelid) ELSE 0 END \
         FROM pg_class c WHERE c.oid = {}::regclass",
        source_regclass(table)
    );
    let mut server = Server::connect(
        address,
        ServerOptions::default(),
        ConnectReason::Resharding,
        Default::default(),
    )
    .await?;
    let row: DataRow = server
        .fetch_all(sql)
        .await?
        .pop()
        .ok_or(Error::MissingData)?;
    let rows: i64 = row.get(0, Format::Text).ok_or(Error::MissingData)?;
    let bytes: i64 = row.get(1, Format::Text).ok_or(Error::MissingData)?;
    Ok(TableEstimate {
        rows: u64::try_from(rows).ok(),
        bytes: bytes as u64,
    })
}

fn source_regclass(table: &Table) -> String {
    quote_literal(&format!(
        "\"{}\".\"{}\"",
        escape_identifier(&table.table.schema),
        escape_identifier(&table.table.name)
    ))
}

/// Returns `true` if any reachable destination shard holds rows from a prior COPY
/// attempt. Emits a single WARN listing the shards that do hold rows.
pub(crate) async fn validate_destination_has_rows(table: &Table, dest: &Cluster) -> bool {
    let schema = table.table.destination_schema();
    let name = table.table.destination_name();
    let sql = format!(
        "SELECT 1 FROM \"{}\".\"{}\" LIMIT 1",
        escape_identifier(schema),
        escape_identifier(name),
    );

    let mut shards_with_rows = vec![];
    for shard in dest.shards().iter() {
        let shard = shard.number();
        let result: Result<bool, Error> = async {
            let mut server = dest.primary(shard, &Request::default()).await?;
            Ok(server
                .execute_checked(sql.as_str())
                .await?
                .iter()
                .any(|m| m.code() == 'D'))
        }
        .await;

        match result {
            Ok(true) => shards_with_rows.push(shard),
            Ok(false) => {}
            Err(err) => warn!(
                "could not verify destination rows on shard {shard}: {err}; \
                     proceeding as if empty"
            ),
        }
    }

    if !shards_with_rows.is_empty() {
        warn!(
            "destination \"{schema}\".\"{name}\" holds rows from a prior COPY attempt on shard(s) {shards_with_rows:?}; \
                 truncate before re-running the copy: TRUNCATE \"{schema}\".\"{name}\";",
        );
    }

    !shards_with_rows.is_empty()
}

/// Check that every table has a usable replica identity.
pub(crate) fn validate_replica_identity(tables: &HashMap<usize, Vec<Table>>) -> Result<(), Error> {
    let validation_errors: Vec<_> = tables
        .values()
        .flat_map(|t| t.iter())
        .filter_map(|t| t.valid().err())
        .collect();

    ensure_validation!(validation_errors);

    Ok(())
}

#[cfg(test)]
mod test {
    use super::super::publisher::publisher_impl::Publisher;
    use super::super::publisher::{PublicationTable, ReplicaIdentity};
    use super::*;
    use crate::backend::replication::publisher::Lsn;
    use crate::backend::server::test::test_replication_server;
    use crate::config::config;
    use pgdog_postgres_types::Oid;

    /// Tables without a primary key or replica identity index must be rejected
    /// before the copy starts, not after, with one entry per bad table.
    #[tokio::test]
    async fn validation_rejects_no_pk_tables() {
        crate::logger();

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

        let source = Cluster::new_test(&config());
        source.launch();

        let mut publisher = Publisher::new("publication_no_pk_validation", "sync_test_slot".into());

        publisher.sync_tables(true, &source).await.unwrap();
        let result = validate_replica_identity(&publisher.tables);

        let err = result.expect_err("validation must fail for a publication with no-pk tables");

        assert_eq!(
            err.to_string(),
            "Table validation failed:\n\
            \ttable \"pgdog\".\"publication_test_no_pk\": has no replica identity columns\n\
            \ttable \"pgdog\".\"publication_test_no_pk_2\": has no replica identity columns\n\
            \ttable \"pgdog\".\"publication_test_no_pk_3\": has no replica identity columns",
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

    /// `REPLICA IDENTITY NOTHING` must be rejected by validation. This test
    /// executes against a real Postgres instance so it validates the full
    /// metadata-fetch + valid() path.
    #[tokio::test]
    async fn validation_rejects_replica_identity_nothing() {
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

        let mut publisher = Publisher::new(
            "pub_full_identity_nothing_test",
            "pub_full_identity_nothing_slot".into(),
        );

        publisher.sync_tables(true, &source).await.unwrap();
        let result = validate_replica_identity(&publisher.tables);

        let err = result.expect_err("validation must fail for REPLICA IDENTITY NOTHING table");
        assert!(
            err.to_string().contains("REPLICA IDENTITY NOTHING"),
            "expected NOTHING in error message, got: {err}"
        );

        source.shutdown();
        for ddl in &[
            "DROP PUBLICATION IF EXISTS pub_full_identity_nothing_test",
            "DROP TABLE IF EXISTS pub_test_nothing",
        ] {
            server.execute(*ddl).await.unwrap();
        }
    }

    /// Leftover rows on the destination must be detected. A shard whose probe
    /// fails cannot be proven dirty and must not be counted.
    #[tokio::test]
    async fn destination_row_check_detects_leftover_rows() {
        crate::logger();

        let mut server = test_replication_server().await;
        for ddl in &[
            "CREATE TABLE IF NOT EXISTS data_sync_leftover_rows (id BIGINT PRIMARY KEY)",
            "TRUNCATE data_sync_leftover_rows",
            "DROP PUBLICATION IF EXISTS pub_data_sync_leftover_rows",
            "CREATE PUBLICATION pub_data_sync_leftover_rows FOR TABLE data_sync_leftover_rows",
        ] {
            server.execute(*ddl).await.unwrap();
        }

        let cluster = Cluster::new_test(&config());
        cluster.launch();

        let mut publisher =
            Publisher::new("pub_data_sync_leftover_rows", "leftover_rows_slot".into());
        publisher.sync_tables(true, &cluster).await.unwrap();
        let table = publisher
            .tables
            .values()
            .flatten()
            .next()
            .expect("publication holds one table")
            .clone();

        assert!(
            !validate_destination_has_rows(&table, &cluster).await,
            "an empty destination must report no rows"
        );

        server
            .execute("INSERT INTO data_sync_leftover_rows VALUES (1)")
            .await
            .unwrap();
        assert!(
            validate_destination_has_rows(&table, &cluster).await,
            "a destination with rows must be detected"
        );

        cluster.shutdown();
        assert!(
            !validate_destination_has_rows(&table, &cluster).await,
            "an unreachable shard cannot be proven dirty"
        );

        for ddl in &[
            "DROP PUBLICATION IF EXISTS pub_data_sync_leftover_rows",
            "DROP TABLE IF EXISTS data_sync_leftover_rows",
        ] {
            server.execute(*ddl).await.unwrap();
        }
    }

    #[tokio::test]
    async fn estimates_track_table_lifecycle() {
        crate::logger();

        let mut server = test_replication_server().await;
        server
            .execute("DROP TABLE IF EXISTS public.data_sync_estimate")
            .await
            .unwrap();

        let table = Table {
            publication: String::new(),
            table: PublicationTable {
                schema: "public".into(),
                name: "data_sync_estimate".into(),
                ..Default::default()
            },
            identity: ReplicaIdentity {
                oid: Oid(0),
                identity: String::new(),
                kind: String::new(),
            },
            columns: vec![],
            lsn: Lsn::default(),
        };

        let addr = Address::new_test();

        assert!(
            estimate_table(&table, &addr).await.is_err(),
            "a missing table must fail the estimate"
        );

        server
            .execute(
                "CREATE TABLE public.data_sync_estimate (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)",
            )
            .await
            .unwrap();
        let fresh = estimate_table(&table, &addr).await.unwrap();
        assert!(
            fresh.rows.is_none(),
            "a never-analyzed table has no row estimate"
        );
        assert_eq!(
            fresh.bytes, 8192,
            "an empty table stores only the TOAST index metapage"
        );

        server
            .execute(
                "INSERT INTO public.data_sync_estimate \
                 SELECT g, repeat('x', 500) FROM generate_series(1, 1000) g",
            )
            .await
            .unwrap();
        server
            .execute("ANALYZE public.data_sync_estimate")
            .await
            .unwrap();
        let after_insert = estimate_table(&table, &addr).await.unwrap();
        assert_eq!(after_insert.rows, Some(1000));
        assert!(
            after_insert.bytes >= 1000 * 500,
            "1000 rows with 500-byte payloads need at least the raw data size: {} bytes",
            after_insert.bytes
        );

        server
            .execute(
                "INSERT INTO public.data_sync_estimate \
                 SELECT g, repeat('y', 500) FROM generate_series(1001, 1500) g",
            )
            .await
            .unwrap();
        server
            .execute("ANALYZE public.data_sync_estimate")
            .await
            .unwrap();
        let after_more = estimate_table(&table, &addr).await.unwrap();
        assert_eq!(after_more.rows, Some(1500));
        assert!(
            after_more.bytes >= 1500 * 500,
            "1500 rows with 500-byte payloads need at least the raw data size: {} bytes",
            after_more.bytes
        );

        server
            .execute("DELETE FROM public.data_sync_estimate")
            .await
            .unwrap();
        server
            .execute("ANALYZE public.data_sync_estimate")
            .await
            .unwrap();
        let after_delete = estimate_table(&table, &addr).await.unwrap();
        assert_eq!(after_delete.rows, Some(0));
        assert!(
            after_delete.bytes <= after_more.bytes,
            "DELETE must not grow the heap; VACUUM may shrink it: {} > {}",
            after_delete.bytes,
            after_more.bytes
        );

        server
            .execute("DROP TABLE public.data_sync_estimate")
            .await
            .unwrap();
        assert!(
            estimate_table(&table, &addr).await.is_err(),
            "a dropped table must fail the estimate"
        );
    }
}
