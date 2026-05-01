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

use super::super::{
    subscriber::CopySubscriber, Error, TableValidationError, TableValidationErrorKind,
};
use super::non_identity_columns_presence::NonIdentityColumnsPresence;
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

/// An enumerated view over a subset of a table's columns.
///
/// Each item is `(original_index, &column)` where `original_index` is the
/// 0-based position of the column in the full column list. This preserves
/// parameter numbers (`$N`) that must line up with a bound value slice
/// containing all columns (INSERT, UPDATE). Call [`Columns::reindexed`]
/// when only the subset's values are bound (DELETE).
struct Columns<'a, I>
where
    I: Iterator<Item = (usize, &'a PublicationTableColumn)>,
{
    inner: I,
}

impl<'a, I> Columns<'a, I>
where
    I: Iterator<Item = (usize, &'a PublicationTableColumn)>,
{
    fn new(inner: I) -> Self {
        Self { inner }
    }

    /// Retain only identity columns, preserving their indices.
    fn filter_identity(
        self,
    ) -> Columns<'a, impl Iterator<Item = (usize, &'a PublicationTableColumn)>> {
        Columns::new(self.inner.filter(|(_, c)| c.identity))
    }

    /// Retain only non-identity columns, preserving their indices.
    fn filter_non_identity(
        self,
    ) -> Columns<'a, impl Iterator<Item = (usize, &'a PublicationTableColumn)>> {
        Columns::new(self.inner.filter(|(_, c)| !c.identity))
    }

    /// Reindex columns sequentially from 0, discarding original positions.
    /// Use when binding only this subset's values ($1 = first column in subset).
    fn reindexed(self) -> Columns<'a, impl Iterator<Item = (usize, &'a PublicationTableColumn)>> {
        Columns::new(self.inner.map(|(_, c)| c).enumerate())
    }

    /// Quoted column names joined by `, `: `"col1", "col2"`.
    fn names(self) -> String {
        self.inner
            .map(|(_, c)| format!("\"{}\"", escape_identifier(&c.name)))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Positional parameter placeholders joined by `, `: `$1, $2`.
    fn placeholders(self) -> String {
        self.inner
            .map(|(i, _)| format!("${}", i + 1))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// `"col" = $pos` pairs joined by `, `. Used in SET clauses.
    fn assignments(self) -> String {
        self.inner
            .map(|(i, c)| format!("\"{}\" = ${}", escape_identifier(&c.name), i + 1))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// `"col" = $pos` pairs joined by ` AND `. Used in WHERE clauses.
    fn predicates(self) -> String {
        self.inner
            .map(|(i, c)| format!("\"{}\" = ${}", escape_identifier(&c.name), i + 1))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    /// Shift every column's parameter index by `offset`.
    /// Use when a preceding clause has already consumed `$1..$offset`.
    fn with_offset(
        self,
        offset: usize,
    ) -> Columns<'a, impl Iterator<Item = (usize, &'a PublicationTableColumn)>> {
        Columns::new(self.inner.map(move |(i, c)| (i + offset, c)))
    }

    /// `"col" IS NOT DISTINCT FROM $pos` predicates joined by ` AND `.
    fn is_not_distinct_from_predicates(self) -> String {
        self.inner
            .map(|(i, c)| {
                format!(
                    "\"{}\" IS NOT DISTINCT FROM ${}",
                    escape_identifier(&c.name),
                    i + 1
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    }
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
    /// - FULL (`"f"`): valid — identity comes from `update.old`/`delete.old`, not column metadata.
    /// - NOTHING (`"n"`): permanently invalid — UPDATE/DELETE carry no row identity.
    /// - DEFAULT/INDEX: valid only when at least one column carries `identity = true`.
    pub fn valid(&self) -> Result<(), TableValidationError> {
        match self.identity.identity.as_str() {
            "f" => Ok(()),
            "n" => Err(TableValidationError {
                table_name: self.table.to_string(),
                kind: TableValidationErrorKind::ReplicaIdentityNothing,
            }),
            _ => {
                if !self.columns.iter().any(|c| c.identity) {
                    return Err(TableValidationError {
                        table_name: self.table.to_string(),
                        kind: TableValidationErrorKind::NoIdentityColumns,
                    });
                }
                Ok(())
            }
        }
    }

    /// Returns all the columns, enumerated
    fn all_columns(&self) -> Columns<'_, impl Iterator<Item = (usize, &PublicationTableColumn)>> {
        Columns::new(self.columns.iter().enumerate())
    }

    /// Returns all the identity columns with their index within all columns
    fn identity_columns(
        &self,
    ) -> Columns<'_, impl Iterator<Item = (usize, &PublicationTableColumn)>> {
        Columns::new(self.columns.iter().enumerate().filter(|(_, c)| c.identity))
    }

    fn non_identity_columns(
        &self,
    ) -> Columns<'_, impl Iterator<Item = (usize, &PublicationTableColumn)>> {
        Columns::new(self.columns.iter().enumerate().filter(|(_, c)| !c.identity))
    }

    /// All columns that should appear in a partial UPDATE for `present`:
    /// identity columns (always) and non-identity columns that are not toasted.
    ///
    /// Returned in original table order with sequential parameter indices
    /// (via [`Columns::reindexed`]), so callers can split into SET and WHERE
    /// parts using [`Columns::filter_non_identity`] and [`Columns::filter_identity`]
    /// while preserving correct `$N` numbering.
    fn present_columns<'a>(
        &'a self,
        present: &'a NonIdentityColumnsPresence,
    ) -> Columns<'a, impl Iterator<Item = (usize, &'a PublicationTableColumn)>> {
        let mut identity_count = 0;

        Columns::new(self.columns.iter().enumerate().filter(move |(i, c)| {
            if c.identity {
                identity_count += 1;
                true
            } else {
                // i - identity_count converts the original column index to the
                // non-identity position that NonIdentityColumnsPresence::is_set expects.
                present.is_set(i - identity_count)
            }
        }))
        .reindexed()
    }

    /// Generate the query for the insertion. Use all the columns in the query.
    /// The related [bind](crate::net::messages::replication::TupleData::to_bind) should set all
    /// the values in the default column order
    pub fn insert(&self) -> String {
        format!(
            "INSERT INTO \"{}\".\"{}\" ({}) VALUES ({})",
            escape_identifier(self.table.destination_schema()),
            escape_identifier(self.table.destination_name()),
            self.all_columns().names(),
            self.all_columns().placeholders(),
        )
    }

    /// Generate the upsert query (INSERT … ON CONFLICT DO UPDATE).
    /// Uses all columns in VALUES; DO UPDATE SET reuses the same `$N` positions — no reindex.
    /// The related [bind](crate::net::messages::replication::TupleData::to_bind) should set all
    /// the values in the default column order
    pub fn upsert(&self) -> String {
        format!(
            "INSERT INTO \"{}\".\"{}\" ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
            escape_identifier(self.table.destination_schema()),
            escape_identifier(self.table.destination_name()),
            self.all_columns().names(),
            self.all_columns().placeholders(),
            self.identity_columns().names(),
            self.non_identity_columns().assignments(),
        )
    }

    /// Generate the UPDATE query for all columns.
    /// Identity columns are in the WHERE part and non-identity in the SET part.
    /// The related [bind](crate::net::messages::replication::TupleData::to_bind) should set all
    /// the values in the default column order
    pub fn update(&self) -> String {
        format!(
            "UPDATE \"{}\".\"{}\" SET {} WHERE {}",
            escape_identifier(self.table.destination_schema()),
            escape_identifier(self.table.destination_name()),
            self.non_identity_columns().assignments(),
            self.identity_columns().predicates(),
        )
    }

    /// Generated partial UPDATE query - the identity column are present in the WHERE part
    /// and the columns that are changed (not unchanged-toasted) in the SET part.
    /// The ordering of columns corresponds with the whole column ordering after filtering
    /// the unchanged-toasted fields.
    ///
    /// Paired with [`Update::partial_new`](crate::net::messages::replication::Update::partial_new) to
    /// generate proper [bind](crate::net::messages::replication::TupleData::to_bind) with proper order.
    pub fn update_partial(&self, present: &NonIdentityColumnsPresence) -> String {
        debug_assert!(
            !present.no_non_identity_present(),
            "update_partial called with no non-identity columns present — would emit empty SET clause"
        );

        format!(
            "UPDATE \"{}\".\"{}\" SET {} WHERE {}",
            escape_identifier(self.table.destination_schema()),
            escape_identifier(self.table.destination_name()),
            self.present_columns(present)
                .filter_non_identity()
                .assignments(),
            self.present_columns(present).filter_identity().predicates(),
        )
    }

    /// Generate the DELETE query filtering by identity columns.
    /// Identity columns are in the WHERE part.
    /// Paired with [`Delete::key_non_null`](crate::net::messages::replication::Delete::key_non_null)
    /// to generate proper [bind](crate::net::messages::replication::TupleData::to_bind) with proper order.
    pub fn delete(&self) -> String {
        format!(
            "DELETE FROM \"{}\".\"{}\" WHERE {}",
            escape_identifier(self.table.destination_schema()),
            escape_identifier(self.table.destination_name()),
            self.identity_columns().reindexed().predicates(),
        )
    }

    /// `INSERT … ON CONFLICT DO NOTHING`. For FULL omni tables; deduplicates overlap-window rows.
    pub fn upsert_full_identity(&self) -> String {
        format!(
            "INSERT INTO \"{}\".\"{}\" ({}) VALUES ({}) ON CONFLICT DO NOTHING",
            escape_identifier(self.table.destination_schema()),
            escape_identifier(self.table.destination_name()),
            self.all_columns().names(),
            self.all_columns().placeholders(),
        )
    }

    /// DELETE for REPLICA IDENTITY FULL tables.
    /// Targets exactly one row via a ctid subquery to handle tables with duplicate rows correctly.
    pub fn delete_full_identity(&self) -> String {
        format!(
            "DELETE FROM \"{}\".\"{}\" WHERE (tableoid, ctid) = {}",
            escape_identifier(self.table.destination_schema()),
            escape_identifier(self.table.destination_name()),
            self.ctid_subquery(),
        )
    }

    /// UPDATE for REPLICA IDENTITY FULL tables — fast path.
    ///
    /// All `n` columns appear twice:
    /// - WHERE `$1..$n`   — ctid subquery using `IS NOT DISTINCT FROM` (all OLD columns)
    /// - SET  `$n+1..$2n` — assignments (all NEW columns)
    ///
    /// Targets exactly one row via ctid to handle tables with duplicate rows correctly.
    /// Bind with `full_identity_bind_tuple(&old_full, &update.new)` → 2n params.
    pub fn update_full_identity(&self) -> String {
        let n = self.columns.len();
        format!(
            "UPDATE \"{}\".\"{}\" SET {} WHERE (tableoid, ctid) = {}",
            escape_identifier(self.table.destination_schema()),
            escape_identifier(self.table.destination_name()),
            self.all_columns().with_offset(n).assignments(),
            self.ctid_subquery(),
        )
    }

    /// UPDATE for REPLICA IDENTITY FULL tables — slow path (some NEW columns are toasted).
    ///
    /// WHERE uses the ctid subquery on all `n` OLD columns; SET uses only the `k` non-toasted NEW columns:
    /// - WHERE `$1..$n`     — ctid subquery with `IS NOT DISTINCT FROM` (all OLD columns)
    /// - SET  `$n+1..$n+k` — assignments for the `k` present columns only
    ///
    /// Targets exactly one row via ctid to handle tables with duplicate rows correctly.
    /// Bind with `full_identity_bind_tuple(&old_full, &partial_new)` → n+k params.
    pub fn update_full_identity_partial_set(&self, present: &NonIdentityColumnsPresence) -> String {
        debug_assert!(
            !present.no_non_identity_present(),
            "update_full_identity_partial_set called with no present columns — would emit empty SET clause"
        );

        let n = self.columns.len();
        format!(
            "UPDATE \"{}\".\"{}\" SET {} WHERE (tableoid, ctid) = {}",
            escape_identifier(self.table.destination_schema()),
            escape_identifier(self.table.destination_name()),
            self.present_columns(present)
                .filter_non_identity()
                .with_offset(n)
                .assignments(),
            self.ctid_subquery(),
        )
    }

    /// `(SELECT tableoid, ctid FROM "s"."t" WHERE col IS NOT DISTINCT FROM $1 AND … LIMIT 1)`
    ///
    /// Used by all FULL identity DELETE/UPDATE generators. `tableoid` scopes the ctid to its
    /// owning heap so the comparison is unambiguous on partitioned destination tables.
    fn ctid_subquery(&self) -> String {
        format!(
            "(SELECT tableoid, ctid FROM \"{}\".\"{}\" WHERE {} LIMIT 1)",
            escape_identifier(self.table.destination_schema()),
            escape_identifier(self.table.destination_name()),
            self.all_columns().is_not_distinct_from_predicates(),
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

    /// `true` when the table uses `REPLICA IDENTITY FULL`.
    pub fn is_identity_full(&self) -> bool {
        self.identity.identity == "f"
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
    use crate::net::messages::replication::logical::tuple_data::{
        text_col, toasted_col, TupleData,
    };

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
        assert!(matches!(
            t.valid(),
            Err(TableValidationError {
                kind: TableValidationErrorKind::NoIdentityColumns,
                ..
            })
        ));
    }

    #[test]
    fn test_sql_generation_simple() {
        let table = make_table(vec![("id", true), ("name", false), ("value", false)]);

        let insert = table.insert();
        assert!(pg_query::parse(&insert).is_ok(), "insert: {}", insert);

        let upsert = table.upsert();
        assert!(pg_query::parse(&upsert).is_ok(), "upsert: {}", upsert);

        let update = table.update();
        assert!(pg_query::parse(&update).is_ok(), "update: {}", update);

        let delete = table.delete();
        assert!(pg_query::parse(&delete).is_ok(), "delete: {}", delete);
    }

    #[test]
    fn test_sql_generation_quoted_column() {
        let table = make_table(vec![("id", true), ("has\"quote", false), ("normal", false)]);

        let insert = table.insert();
        assert!(pg_query::parse(&insert).is_ok(), "insert: {}", insert);

        let upsert = table.upsert();
        assert!(pg_query::parse(&upsert).is_ok(), "upsert: {}", upsert);

        let update = table.update();
        assert!(pg_query::parse(&update).is_ok(), "update: {}", update);

        let delete = table.delete();
        assert!(pg_query::parse(&delete).is_ok(), "delete: {}", delete);
    }

    #[test]
    fn test_delete_sequential_params_identity_not_first() {
        // reindexed() always starts from $1 regardless of the column's position in the row.
        let table = make_table(vec![("name", false), ("value", false), ("id", true)]);
        assert_eq!(
            table.delete(),
            r#"DELETE FROM "public"."test_table" WHERE "id" = $1"#
        );
    }

    #[test]
    fn test_delete_sequential_params_composite_key() {
        // Non-contiguous identity columns (idx 0, 2) reindex to $1, $2; non-identity never appears.
        let table = make_table(vec![("id", true), ("name", false), ("version", true)]);
        assert_eq!(
            table.delete(),
            r#"DELETE FROM "public"."test_table" WHERE "id" = $1 AND "version" = $2"#,
        );
    }

    #[test]
    fn test_delete_identity_first_correct() {
        let table = make_table(vec![("id", true), ("name", false)]);
        assert_eq!(
            table.delete(),
            r#"DELETE FROM "public"."test_table" WHERE "id" = $1"#
        );
    }

    #[test]
    fn test_update_param_numbering_identity_trailing() {
        let table = make_table(vec![("name", false), ("id", true)]);
        assert_eq!(
            table.update(),
            r#"UPDATE "public"."test_table" SET "name" = $1 WHERE "id" = $2"#,
        );
    }

    #[test]
    fn test_update_param_numbering_identity_leading() {
        let table = make_table(vec![("id", true), ("name", false), ("value", false)]);
        assert_eq!(
            table.update(),
            r#"UPDATE "public"."test_table" SET "name" = $2, "value" = $3 WHERE "id" = $1"#,
        );
    }

    #[test]
    fn test_upsert_param_numbering_identity_trailing() {
        // DO UPDATE SET reuses the original non-identity positions; same $N as in VALUES.
        let table = make_table(vec![("name", false), ("value", false), ("id", true)]);
        assert_eq!(
            table.upsert(),
            r#"INSERT INTO "public"."test_table" ("name", "value", "id") VALUES ($1, $2, $3) ON CONFLICT ("id") DO UPDATE SET "name" = $1, "value" = $2"#,
        );
    }

    #[test]
    fn test_upsert_param_numbering_identity_leading() {
        let table = make_table(vec![("id", true), ("name", false), ("value", false)]);
        assert_eq!(
            table.upsert(),
            r#"INSERT INTO "public"."test_table" ("id", "name", "value") VALUES ($1, $2, $3) ON CONFLICT ("id") DO UPDATE SET "name" = $2, "value" = $3"#,
        );
    }

    #[test]
    fn test_sql_generation_special_chars() {
        let table = make_table(vec![
            ("id", true),
            ("col with spaces", false),
            ("UPPER", false),
        ]);

        let insert = table.insert();
        assert!(pg_query::parse(&insert).is_ok(), "insert: {}", insert);

        let upsert = table.upsert();
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

        let insert = table.insert();
        assert!(pg_query::parse(&insert).is_ok(), "insert: {}", insert);

        let upsert = table.upsert();
        assert!(pg_query::parse(&upsert).is_ok(), "upsert: {}", upsert);

        let update = table.update();
        assert!(pg_query::parse(&update).is_ok(), "update: {}", update);

        let delete = table.delete();
        assert!(pg_query::parse(&delete).is_ok(), "delete: {}", delete);
    }

    #[test]
    fn update_partial_all_columns() {
        let table = make_table(vec![("id", true), ("a", false), ("b", false)]);
        let present = NonIdentityColumnsPresence::all(&table);
        assert_eq!(
            table.update_partial(&present),
            r#"UPDATE "public"."test_table" SET "a" = $2, "b" = $3 WHERE "id" = $1"#,
        );
    }

    #[test]
    fn update_partial_skips_toasted_column() {
        let table = make_table(vec![("id", true), ("a", false), ("b", false), ("c", false)]);
        let tuple = TupleData {
            columns: vec![text_col("1"), text_col("x"), toasted_col(), text_col("z")],
        };
        let present = NonIdentityColumnsPresence::from_tuple(&tuple, &table).unwrap();
        assert_eq!(
            table.update_partial(&present),
            r#"UPDATE "public"."test_table" SET "a" = $2, "c" = $3 WHERE "id" = $1"#,
        );
    }

    #[test]
    fn update_partial_identity_not_first() {
        // Identity column last — params must still be sequential.
        let table = make_table(vec![("name", false), ("value", false), ("id", true)]);
        let present = NonIdentityColumnsPresence::all(&table);
        assert_eq!(
            table.update_partial(&present),
            r#"UPDATE "public"."test_table" SET "name" = $1, "value" = $2 WHERE "id" = $3"#,
        );
    }

    #[test]
    fn update_partial_identity_in_middle() {
        // Identity column sits between two non-identity columns.
        // The SET column before identity gets $1; identity gets $2;
        // the SET column after identity gets $3.
        let table = make_table(vec![("a", false), ("id", true), ("b", false)]);
        let present = NonIdentityColumnsPresence::all(&table);
        assert_eq!(
            table.update_partial(&present),
            r#"UPDATE "public"."test_table" SET "a" = $1, "b" = $3 WHERE "id" = $2"#,
        );
    }

    #[test]
    fn update_partial_multiple_identity_columns() {
        // Compound primary key: two identity columns followed by two non-identity columns.
        // Identity columns occupy $1 and $2; non-identity columns follow at $3 and $4.
        let table = make_table(vec![
            ("id1", true),
            ("id2", true),
            ("a", false),
            ("b", false),
        ]);
        let present = NonIdentityColumnsPresence::all(&table);
        assert_eq!(
            table.update_partial(&present),
            r#"UPDATE "public"."test_table" SET "a" = $3, "b" = $4 WHERE "id1" = $1 AND "id2" = $2"#,
        );
    }

    #[test]
    fn update_partial_first_non_identity_toasted() {
        // Non-identity column at position 0 is toasted; identity is last.
        // Only the second non-identity column is present, so it gets $1
        // and identity follows at $2.
        let table = make_table(vec![("a", false), ("b", false), ("id", true)]);
        let tuple = TupleData {
            columns: vec![toasted_col(), text_col("bv"), text_col("1")],
        };
        let present = NonIdentityColumnsPresence::from_tuple(&tuple, &table).unwrap();
        assert_eq!(
            table.update_partial(&present),
            r#"UPDATE "public"."test_table" SET "b" = $1 WHERE "id" = $2"#,
        );
    }

    #[test]
    fn update_partial_two_identity_interleaved_two_toasted() {
        // Table: a, id1 (identity), b, c, id2 (identity).
        // a and c are toasted; only b survives the non-identity filter.
        // present_columns produces [(0,id1),(1,b),(2,id2)] after reindexing,
        // so b lands at $2 between the two identity params.
        let table = make_table(vec![
            ("a", false),
            ("id1", true),
            ("b", false),
            ("c", false),
            ("id2", true),
        ]);
        let tuple = TupleData {
            columns: vec![
                toasted_col(),  // a — unchanged TOAST
                text_col("1"),  // id1
                text_col("bv"), // b
                toasted_col(),  // c — unchanged TOAST
                text_col("2"),  // id2
            ],
        };
        let present = NonIdentityColumnsPresence::from_tuple(&tuple, &table).unwrap();
        assert_eq!(
            table.update_partial(&present),
            r#"UPDATE "public"."test_table" SET "b" = $2 WHERE "id1" = $1 AND "id2" = $3"#,
        );
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
            let upsert = table.upsert();
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
            Err(TableValidationError {
                kind: TableValidationErrorKind::NoIdentityColumns,
                ..
            })
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
        assert!(table.valid().is_ok());
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
        assert!(matches!(
            table.valid(),
            Err(TableValidationError {
                kind: TableValidationErrorKind::ReplicaIdentityNothing,
                ..
            })
        ));
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

    // -------------------------------------------------------------------
    // REPLICA IDENTITY FULL SQL generators
    // -------------------------------------------------------------------

    #[test]
    fn upsert_full_identity_is_valid_sql() {
        let table = make_table(vec![("a", false), ("b", false), ("c", false)]);
        let sql = table.upsert_full_identity();
        assert_eq!(
            sql,
            r#"INSERT INTO "public"."test_table" ("a", "b", "c") VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"#,
        );
    }

    #[test]
    fn delete_full_identity_param_numbering() {
        // ctid subquery targets exactly one row; param order matches column order.
        let table = make_table(vec![("a", false), ("b", false), ("c", false)]);
        let sql = table.delete_full_identity();
        let (q, s, t) = ('"', "public", "test_table");
        let pred = format!("{q}a{q} IS NOT DISTINCT FROM $1 AND {q}b{q} IS NOT DISTINCT FROM $2 AND {q}c{q} IS NOT DISTINCT FROM $3");
        let subq = format!("(SELECT tableoid, ctid FROM {q}{s}{q}.{q}{t}{q} WHERE {pred} LIMIT 1)");
        assert_eq!(
            sql,
            format!("DELETE FROM {q}{s}{q}.{q}{t}{q} WHERE (tableoid, ctid) = {subq}")
        );
        assert!(pg_query::parse(&sql).is_ok(), "delete_full_identity: {sql}");
    }

    #[test]
    fn delete_full_identity_single_column() {
        // Edge case: one-column table — subquery WHERE must still be well-formed.
        let table = make_table(vec![("id", false)]);
        let sql = table.delete_full_identity();
        let (q, s, t) = ('"', "public", "test_table");
        let pred = format!("{q}id{q} IS NOT DISTINCT FROM $1");
        let subq = format!("(SELECT tableoid, ctid FROM {q}{s}{q}.{q}{t}{q} WHERE {pred} LIMIT 1)");
        assert_eq!(
            sql,
            format!("DELETE FROM {q}{s}{q}.{q}{t}{q} WHERE (tableoid, ctid) = {subq}")
        );
        assert!(
            pg_query::parse(&sql).is_ok(),
            "delete_full_identity single: {sql}"
        );
    }

    #[test]
    fn update_full_identity_all_present() {
        // All columns present (no TOAST). WHERE in ctid subquery: $1..$3; SET: $4..$6.
        let table = make_table(vec![("a", false), ("b", false), ("c", false)]);
        let sql = table.update_full_identity();
        let (q, s, t) = ('"', "public", "test_table");
        let pred = format!("{q}a{q} IS NOT DISTINCT FROM $1 AND {q}b{q} IS NOT DISTINCT FROM $2 AND {q}c{q} IS NOT DISTINCT FROM $3");
        let subq = format!("(SELECT tableoid, ctid FROM {q}{s}{q}.{q}{t}{q} WHERE {pred} LIMIT 1)");
        let set = format!("{q}a{q} = $4, {q}b{q} = $5, {q}c{q} = $6 ");
        assert_eq!(
            sql,
            format!("UPDATE {q}{s}{q}.{q}{t}{q} SET {set}WHERE (tableoid, ctid) = {subq}")
        );
        assert!(
            pg_query::parse(&sql).is_ok(),
            "update_full_identity all_present: {sql}"
        );
    }

    #[test]
    fn update_full_identity_partial_present() {
        // b is Toasted. WHERE in ctid subquery: all 3 ($1..$3); SET: a,c ($4..$5).
        let table = make_table(vec![("a", false), ("b", false), ("c", false)]);
        let tuple = TupleData {
            columns: vec![text_col("1"), toasted_col(), text_col("3")],
        };
        let present = NonIdentityColumnsPresence::from_tuple(&tuple, &table).unwrap();
        let sql = table.update_full_identity_partial_set(&present);
        let (q, s, t) = ('"', "public", "test_table");
        let pred = format!("{q}a{q} IS NOT DISTINCT FROM $1 AND {q}b{q} IS NOT DISTINCT FROM $2 AND {q}c{q} IS NOT DISTINCT FROM $3");
        let subq = format!("(SELECT tableoid, ctid FROM {q}{s}{q}.{q}{t}{q} WHERE {pred} LIMIT 1)");
        let set = format!("{q}a{q} = $4, {q}c{q} = $5 ");
        assert_eq!(
            sql,
            format!("UPDATE {q}{s}{q}.{q}{t}{q} SET {set}WHERE (tableoid, ctid) = {subq}")
        );
        assert!(
            pg_query::parse(&sql).is_ok(),
            "update_full_identity partial: {sql}"
        );
    }

    #[test]
    fn update_full_identity_first_column_toasted() {
        // a is Toasted; b and c present. WHERE in ctid subquery: all 3 ($1..$3); SET: b,c ($4..$5).
        let table = make_table(vec![("a", false), ("b", false), ("c", false)]);
        let tuple = TupleData {
            columns: vec![toasted_col(), text_col("2"), text_col("3")],
        };
        let present = NonIdentityColumnsPresence::from_tuple(&tuple, &table).unwrap();
        let sql = table.update_full_identity_partial_set(&present);
        let (q, s, t) = ('"', "public", "test_table");
        let pred = format!("{q}a{q} IS NOT DISTINCT FROM $1 AND {q}b{q} IS NOT DISTINCT FROM $2 AND {q}c{q} IS NOT DISTINCT FROM $3");
        let subq = format!("(SELECT tableoid, ctid FROM {q}{s}{q}.{q}{t}{q} WHERE {pred} LIMIT 1)");
        let set = format!("{q}b{q} = $4, {q}c{q} = $5 ");
        assert_eq!(
            sql,
            format!("UPDATE {q}{s}{q}.{q}{t}{q} SET {set}WHERE (tableoid, ctid) = {subq}")
        );
        assert!(
            pg_query::parse(&sql).is_ok(),
            "update_full_identity first_toasted: {sql}"
        );
    }

    #[test]
    fn update_full_identity_only_one_column_present() {
        // Only c is present. WHERE in ctid subquery: all 3 ($1..$3); SET: c ($4).
        let table = make_table(vec![("a", false), ("b", false), ("c", false)]);
        let tuple = TupleData {
            columns: vec![toasted_col(), toasted_col(), text_col("3")],
        };
        let present = NonIdentityColumnsPresence::from_tuple(&tuple, &table).unwrap();
        let sql = table.update_full_identity_partial_set(&present);
        let (q, s, t) = ('"', "public", "test_table");
        let pred = format!("{q}a{q} IS NOT DISTINCT FROM $1 AND {q}b{q} IS NOT DISTINCT FROM $2 AND {q}c{q} IS NOT DISTINCT FROM $3");
        let subq = format!("(SELECT tableoid, ctid FROM {q}{s}{q}.{q}{t}{q} WHERE {pred} LIMIT 1)");
        let set = format!("{q}c{q} = $4 ");
        assert_eq!(
            sql,
            format!("UPDATE {q}{s}{q}.{q}{t}{q} SET {set}WHERE (tableoid, ctid) = {subq}")
        );
        assert!(
            pg_query::parse(&sql).is_ok(),
            "update_full_identity single col: {sql}"
        );
    }
}
