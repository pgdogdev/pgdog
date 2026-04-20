//! Table.

use std::time::Duration;

use pgdog_config::QueryParserEngine;
use tokio::select;
use tracing::error;

use crate::backend::pool::Address;
use crate::backend::replication::publisher::progress::Progress;
use crate::backend::replication::publisher::Lsn;

use crate::backend::pool::Request;
use crate::backend::replication::status::TableCopy;
use crate::backend::{Cluster, Server, ShardedTables};
use crate::config::config;
use crate::frontend::router::parser::Column;
use crate::net::messages::Protocol;
use crate::net::replication::StatusUpdate;
use crate::util::escape_identifier;

use super::super::{subscriber::CopySubscriber, Error};
use super::{
    AbortSignal, Copy, PublicationTable, PublicationTableColumn, ReplicaIdentity, ReplicationSlot,
};

use tracing::info;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Table {
    /// Name of the table publication.
    pub publication: String,
    /// Table data.
    pub table: PublicationTable,
    /// Table replica identity.
    pub identity: ReplicaIdentity,
    /// Table columns.
    pub columns: Vec<PublicationTableColumn>,
    /// Table data as of this LSN.
    pub lsn: Lsn,
    /// Query parser engine.
    pub query_parser_engine: QueryParserEngine,
}

impl Table {
    pub async fn load(
        publication: &str,
        server: &mut Server,
        query_parser_engine: QueryParserEngine,
    ) -> Result<Vec<Self>, Error> {
        let tables = PublicationTable::load(publication, server).await?;
        let mut results = vec![];

        for table in tables {
            let identity = ReplicaIdentity::load(&table, server).await?;
            let columns = PublicationTableColumn::load(&identity, server).await?;

            results.push(Self {
                publication: publication.to_owned(),
                table: table.clone(),
                identity,
                columns,
                lsn: Lsn::default(),
                query_parser_engine,
            });
        }

        Ok(results)
    }

    /// Key used for duplicate check.
    pub(super) fn key(&self) -> (String, String) {
        (self.table.schema.clone(), self.table.name.clone())
    }

    /// Check that the table supports replication.
    ///
    /// Requires at least one column with a replica identity flag. Tables with
    /// REPLICA IDENTITY FULL or NOTHING have no identity columns and fail here
    /// with NoPrimaryKey.
    pub fn valid(&self) -> Result<(), Error> {
        if !self.columns.iter().any(|c| c.identity) {
            return Err(Error::NoPrimaryKey(self.table.clone()));
        }
        Ok(())
    }

    /// Upsert record into table.
    pub fn insert(&self, upsert: bool) -> String {
        let names = format!(
            "({})",
            self.columns
                .iter()
                .map(|c| format!("\"{}\"", escape_identifier(&c.name)))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let values = format!(
            "VALUES ({})",
            self.columns
                .iter()
                .enumerate()
                .map(|(i, _)| format!("${}", i + 1))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let on_conflict = if upsert {
            format!(
                "ON CONFLICT ({}) DO UPDATE SET {}",
                self.columns
                    .iter()
                    .filter(|c| c.identity)
                    .map(|c| format!("\"{}\"", escape_identifier(&c.name)))
                    .collect::<Vec<_>>()
                    .join(", "),
                self.columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| !c.identity)
                    .map(|(i, c)| format!("\"{}\" = ${}", escape_identifier(&c.name), i + 1))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            "".to_string()
        };

        format!(
            "INSERT INTO \"{}\".\"{}\" {} {} {}",
            escape_identifier(self.table.destination_schema()),
            escape_identifier(self.table.destination_name()),
            names,
            values,
            on_conflict
        )
    }

    /// Update record in table.
    pub fn update(&self) -> String {
        let set_clause = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.identity)
            .map(|(i, c)| format!("\"{}\" = ${}", escape_identifier(&c.name), i + 1))
            .collect::<Vec<_>>()
            .join(", ");

        let where_clause = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.identity)
            .map(|(i, c)| format!("\"{}\" = ${}", escape_identifier(&c.name), i + 1))
            .collect::<Vec<_>>()
            .join(" AND ");

        format!(
            "UPDATE \"{}\".\"{}\" SET {} WHERE {}",
            escape_identifier(self.table.destination_schema()),
            escape_identifier(self.table.destination_name()),
            set_clause,
            where_clause
        )
    }

    /// Delete record from table.
    pub fn delete(&self) -> String {
        let where_clause = self
            .columns
            .iter()
            .filter(|c| c.identity)
            .enumerate()
            .map(|(i, c)| format!("\"{}\" = ${}", escape_identifier(&c.name), i + 1))
            .collect::<Vec<_>>()
            .join(" AND ");

        format!(
            "DELETE FROM \"{}\".\"{}\" WHERE {}",
            escape_identifier(self.table.destination_schema()),
            escape_identifier(self.table.destination_name()),
            where_clause
        )
    }

    /// Reload table data inside the transaction.
    pub async fn reload(&mut self, server: &mut Server) -> Result<(), Error> {
        if !server.in_transaction() {
            return Err(Error::TransactionNotStarted);
        }

        self.identity = ReplicaIdentity::load(&self.table, server).await?;
        self.columns = PublicationTableColumn::load(&self.identity, server).await?;

        Ok(())
    }

    /// Check if this table is sharded.
    pub fn is_sharded(&self, tables: &ShardedTables) -> bool {
        for column in &self.columns {
            let c = Column {
                name: &column.name,
                table: Some(&self.table.name),
                schema: Some(&self.table.schema),
            };

            if tables.get_table(c).is_some() {
                return true;
            }
        }

        false
    }

    pub async fn data_sync(
        &mut self,
        source: &Address,
        dest: &Cluster,
        abort: AbortSignal,
        tracker: &TableCopy,
    ) -> Result<Lsn, Error> {
        info!(
            "data sync for \"{}\".\"{}\" started [{}]",
            self.table.schema, self.table.name, source
        );

        // Sync data using COPY.
        // Publisher uses COPY [...] TO STDOUT.
        // Subscriber uses COPY [...] FROM STDIN.
        let copy = Copy::new(self, config().config.general.resharding_copy_format);

        tracker.update_sql(&copy.statement().copy_out());

        // Create new standalone connection for the copy.
        // let mut server = Server::connect(source, ServerOptions::new_replication()).await?;
        let mut copy_sub = CopySubscriber::new(copy.statement(), dest, self.query_parser_engine)?;
        copy_sub.connect().await?;

        // Create sync slot.
        let mut slot = ReplicationSlot::data_sync(&self.publication, source);
        slot.connect().await?;
        self.lsn = slot.create_slot().await?;

        // Reload table info just to be sure it's consistent.
        self.reload(slot.server()?).await?;

        // Copy rows over.
        copy.start(slot.server()?).await?;
        copy_sub.start_copy().await?;
        let progress = Progress::new_data_sync(&self.table);

        while let Some(data_row) = copy.data(slot.server()?).await? {
            select! {
                _ = abort.aborted() =>  {
                    error!("aborting data sync for table {}", self.table);

                    return Err(Error::CopyAborted(self.table.clone()))
                },

                result = copy_sub.copy_data(data_row) => {
                    let (rows, bytes) = result?;
                    progress.update(copy_sub.bytes_sharded(), slot.lsn().lsn);
                    tracker.update_progress(bytes, rows);
                }
            }
        }

        copy_sub.copy_done().await?;

        copy_sub.disconnect().await?;
        progress.done();

        slot.server()?.execute("COMMIT").await?;

        // Close slot.
        slot.start_replication().await?;
        slot.status_update(StatusUpdate::new_reply(self.lsn))
            .await?;
        slot.stop_replication().await?;

        // Drain slot
        while slot.replicate(Duration::MAX).await?.is_some() {}

        // Slot is temporary and will be dropped when the connection closes.

        info!(
            "data sync for \"{}\".\"{}\" finished at lsn {} [{}]",
            self.table.schema, self.table.name, self.lsn, source
        );

        Ok(self.lsn)
    }

    /// Returns `true` if the destination table has any rows on any shard.
    ///
    /// COPY is transactional and normally auto-rolls back on failure, leaving
    /// the destination empty. This check catches the rare race where COPY
    /// committed but an error was returned afterward (e.g., network drop during
    /// CommandComplete), resulting in rows the retry would collide with.
    pub async fn destination_has_rows(&self, dest: &Cluster) -> Result<bool, Error> {
        let sql = format!(
            "SELECT 1 FROM \"{}\".\"{}\" LIMIT 1",
            escape_identifier(self.table.destination_schema()),
            escape_identifier(self.table.destination_name()),
        );
        for (shard, _) in dest.shards().iter().enumerate() {
            let mut server = dest.primary(shard, &Request::default()).await?;
            let messages = server.execute_checked(sql.as_str()).await?;
            if messages.iter().any(|m| m.code() == 'D') {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod test {
    use crate::backend::replication::logical::publisher::test::setup_publication;
    use crate::backend::{
        replication::logical::publisher::queries::{PublicationTableColumn, ReplicaIdentity},
        server::test::test_server,
        Server,
    };
    use crate::config::config;

    use super::*;

    fn make_table(columns: Vec<(&str, bool)>) -> Table {
        Table {
            publication: "test".to_string(),
            table: PublicationTable {
                schema: "public".to_string(),
                name: "test_table".to_string(),
                attributes: "".to_string(),
                parent_schema: "".to_string(),
                parent_name: "".to_string(),
            },
            identity: ReplicaIdentity {
                oid: pgdog_postgres_types::Oid(1),
                identity: "".to_string(),
                kind: "".to_string(),
            },
            columns: columns
                .into_iter()
                .map(|(name, identity)| PublicationTableColumn {
                    oid: 1,
                    name: name.to_string(),
                    type_oid: pgdog_postgres_types::Oid(23),
                    identity,
                })
                .collect(),
            lsn: Lsn::default(),
            query_parser_engine: QueryParserEngine::default(),
        }
    }

    #[test]
    fn valid_with_pk() {
        let t = make_table(vec![("id", true), ("name", false)]);
        assert!(t.valid().is_ok());
    }

    #[test]
    fn valid_without_pk() {
        let t = make_table(vec![("id", false), ("name", false)]);
        assert!(matches!(t.valid(), Err(Error::NoPrimaryKey(_))));
    }

    #[test]
    fn test_sql_generation_simple() {
        let table = make_table(vec![("id", true), ("name", false), ("value", false)]);

        let insert = table.insert(false);
        assert!(pg_query::parse(&insert).is_ok(), "insert: {}", insert);

        let upsert = table.insert(true);
        assert!(pg_query::parse(&upsert).is_ok(), "upsert: {}", upsert);

        let update = table.update();
        assert!(pg_query::parse(&update).is_ok(), "update: {}", update);

        let delete = table.delete();
        assert!(pg_query::parse(&delete).is_ok(), "delete: {}", delete);
    }

    #[test]
    fn test_sql_generation_quoted_column() {
        let table = make_table(vec![("id", true), ("has\"quote", false), ("normal", false)]);

        let insert = table.insert(false);
        assert!(pg_query::parse(&insert).is_ok(), "insert: {}", insert);

        let upsert = table.insert(true);
        assert!(pg_query::parse(&upsert).is_ok(), "upsert: {}", upsert);

        let update = table.update();
        assert!(pg_query::parse(&update).is_ok(), "update: {}", update);

        let delete = table.delete();
        assert!(pg_query::parse(&delete).is_ok(), "delete: {}", delete);
    }

    #[test]
    fn test_delete_sequential_params_identity_not_first() {
        // Regression: when identity columns aren't at the start of the column list,
        // delete() must still produce sequential $1, $2, ... parameters.
        let table = make_table(vec![("name", false), ("value", false), ("id", true)]);
        let delete = table.delete();
        assert!(delete.contains("$1"), "expected $1 in delete: {}", delete);
        assert!(
            !delete.contains("$3"),
            "delete should not skip to $3: {}",
            delete
        );
        assert!(pg_query::parse(&delete).is_ok(), "delete: {}", delete);
    }

    #[test]
    fn test_delete_sequential_params_composite_key() {
        // Regression: composite key with non-contiguous identity columns
        // must produce $1, $2 not $1, $3.
        let table = make_table(vec![("id", true), ("name", false), ("version", true)]);
        let delete = table.delete();
        assert!(
            delete.contains("$1") && delete.contains("$2"),
            "expected $1 and $2 in delete: {}",
            delete
        );
        assert!(
            !delete.contains("$3"),
            "delete should not reference $3: {}",
            delete
        );
        assert!(pg_query::parse(&delete).is_ok(), "delete: {}", delete);
    }

    #[test]
    fn test_sql_generation_special_chars() {
        let table = make_table(vec![
            ("id", true),
            ("col with spaces", false),
            ("UPPER", false),
        ]);

        let insert = table.insert(false);
        assert!(pg_query::parse(&insert).is_ok(), "insert: {}", insert);

        let upsert = table.insert(true);
        assert!(pg_query::parse(&upsert).is_ok(), "upsert: {}", upsert);

        let update = table.update();
        assert!(pg_query::parse(&update).is_ok(), "update: {}", update);

        let delete = table.delete();
        assert!(pg_query::parse(&delete).is_ok(), "delete: {}", delete);
    }

    #[test]
    fn test_sql_generation_quoted_table_name() {
        let mut table = make_table(vec![("id", true), ("value", false)]);
        table.table.name = "table\"with\"quotes".to_string();
        table.table.schema = "schema\"quote".to_string();

        let insert = table.insert(false);
        assert!(pg_query::parse(&insert).is_ok(), "insert: {}", insert);

        let upsert = table.insert(true);
        assert!(pg_query::parse(&upsert).is_ok(), "upsert: {}", upsert);

        let update = table.update();
        assert!(pg_query::parse(&update).is_ok(), "update: {}", update);

        let delete = table.delete();
        assert!(pg_query::parse(&delete).is_ok(), "delete: {}", delete);
    }

    #[tokio::test]
    async fn test_publication() {
        crate::logger();

        let mut publication = setup_publication().await;
        let tables = Table::load(
            "publication_test",
            &mut publication.server,
            config().config.general.query_parser_engine,
        )
        .await
        .unwrap();

        assert_eq!(tables.len(), 2);

        for table in tables {
            let upsert = table.insert(true);
            assert!(pg_query::parse(&upsert).is_ok());

            let update = table.update();
            assert!(pg_query::parse(&update).is_ok());

            let delete = table.delete();
            assert!(pg_query::parse(&delete).is_ok());
        }

        publication.cleanup().await;
    }

    // Load identity + columns for a named table that already exists in the current session.
    // Uses pg_catalog directly — no publication required.
    async fn load_table(server: &mut Server, name: &str) -> Table {
        let pub_table = PublicationTable {
            schema: "public".into(),
            name: name.into(),
            attributes: "".into(),
            parent_schema: "".into(),
            parent_name: "".into(),
        };
        let identity = ReplicaIdentity::load(&pub_table, server).await.unwrap();
        let columns = PublicationTableColumn::load(&identity, server)
            .await
            .unwrap();
        Table {
            publication: "".into(),
            table: pub_table,
            identity,
            columns,
            lsn: Lsn::default(),
            query_parser_engine: QueryParserEngine::default(),
        }
    }

    #[tokio::test]
    async fn test_valid_pk() {
        crate::logger();
        let mut s = test_server().await;
        s.execute("BEGIN").await.unwrap();
        s.execute("CREATE TABLE public.valid_test_pk (id BIGSERIAL PRIMARY KEY, name TEXT)")
            .await
            .unwrap();
        assert!(load_table(&mut s, "valid_test_pk").await.valid().is_ok());
        s.execute("ROLLBACK").await.unwrap();
    }

    #[tokio::test]
    async fn test_valid_no_pk() {
        crate::logger();
        let mut s = test_server().await;
        s.execute("BEGIN").await.unwrap();
        s.execute("CREATE TABLE public.valid_test_nopk (id BIGINT, name TEXT)")
            .await
            .unwrap();
        assert!(matches!(
            load_table(&mut s, "valid_test_nopk").await.valid(),
            Err(Error::NoPrimaryKey(_))
        ));
        s.execute("ROLLBACK").await.unwrap();
    }

    #[tokio::test]
    async fn test_valid_replica_identity_full() {
        crate::logger();
        let mut s = test_server().await;
        s.execute("BEGIN").await.unwrap();
        s.execute("CREATE TABLE public.valid_test_full (id BIGINT, name TEXT)")
            .await
            .unwrap();
        s.execute("ALTER TABLE valid_test_full REPLICA IDENTITY FULL")
            .await
            .unwrap();
        let table = load_table(&mut s, "valid_test_full").await;
        assert_eq!(table.identity.identity, "f");
        assert!(matches!(table.valid(), Err(Error::NoPrimaryKey(_))));
        s.execute("ROLLBACK").await.unwrap();
    }

    #[tokio::test]
    async fn test_valid_replica_identity_nothing() {
        crate::logger();
        let mut s = test_server().await;
        s.execute("BEGIN").await.unwrap();
        s.execute("CREATE TABLE public.valid_test_nothing (id BIGSERIAL PRIMARY KEY, name TEXT)")
            .await
            .unwrap();
        s.execute("ALTER TABLE valid_test_nothing REPLICA IDENTITY NOTHING")
            .await
            .unwrap();
        let table = load_table(&mut s, "valid_test_nothing").await;
        assert_eq!(table.identity.identity, "n");
        assert!(matches!(table.valid(), Err(Error::NoPrimaryKey(_))));
        s.execute("ROLLBACK").await.unwrap();
    }

    #[tokio::test]
    async fn test_valid_replica_identity_using_index() {
        crate::logger();
        let mut s = test_server().await;
        s.execute("BEGIN").await.unwrap();
        s.execute("CREATE TABLE public.valid_test_idx (email TEXT NOT NULL, name TEXT)")
            .await
            .unwrap();
        s.execute("CREATE UNIQUE INDEX valid_test_idx_uidx ON valid_test_idx (email)")
            .await
            .unwrap();
        s.execute("ALTER TABLE valid_test_idx REPLICA IDENTITY USING INDEX valid_test_idx_uidx")
            .await
            .unwrap();
        let table = load_table(&mut s, "valid_test_idx").await;
        assert_eq!(table.identity.identity, "i");
        assert!(table.valid().is_ok());
        s.execute("ROLLBACK").await.unwrap();
    }
}
