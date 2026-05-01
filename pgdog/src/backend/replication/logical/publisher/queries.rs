//! Queries to fetch publication info.
//!
//! TODO: I think these are Postgres-version specific, so we need to handle that
//! later. These were fetched from CREATE SUBSCRIPTION ran on Postgres 17.
//!
use std::{collections::HashSet, fmt::Display};

use pgdog_postgres_types::Oid;

use crate::{
    backend::Server,
    net::{DataRow, Format},
};

use super::super::Error;

fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Get list of tables in publication.
static TABLES: &str = "SELECT DISTINCT
  n.nspname,
  c.relname,
  gpt.attrs,
  COALESCE(pn.nspname::text, '') AS parent_schema,
  COALESCE(p.relname::text, '')  AS parent_table
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN (
  SELECT (pg_get_publication_tables(VARIADIC array_agg(pubname::text))).*
  FROM pg_publication
  WHERE pubname IN ($1)
) AS gpt
  ON gpt.relid = c.oid
LEFT JOIN pg_inherits i     ON i.inhrelid = c.oid           -- only present if c is a child partition
LEFT JOIN pg_class    p     ON p.oid = i.inhparent          -- immediate parent partitioned table
LEFT JOIN pg_namespace pn   ON pn.oid = p.relnamespace
ORDER BY n.nspname, c.relname;";

/// Table included in a publication.
#[derive(Debug, Clone, PartialEq, Default, Eq, Hash)]
pub struct PublicationTable {
    pub schema: String,
    pub name: String,
    pub attributes: String,
    pub parent_schema: String,
    pub parent_name: String,
}

impl Display for PublicationTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "\"{}\".\"{}\"", self.schema, self.name)
    }
}

impl PublicationTable {
    pub async fn load(
        publication: &str,
        server: &mut Server,
    ) -> Result<Vec<PublicationTable>, Error> {
        // fetch_all (simple query protocol) is required: replication connections
        // do not support the extended query protocol (error 08P01).
        Ok(server
            .fetch_all(TABLES.replace("$1", &quote_literal(publication)))
            .await?)
    }

    pub fn destination_name(&self) -> &str {
        if self.parent_name.is_empty() {
            &self.name
        } else {
            &self.parent_name
        }
    }

    pub fn destination_schema(&self) -> &str {
        if self.parent_schema.is_empty() {
            &self.schema
        } else {
            &self.parent_schema
        }
    }
}

impl From<DataRow> for PublicationTable {
    fn from(value: DataRow) -> Self {
        Self {
            schema: value.get(0, Format::Text).unwrap_or_default(),
            name: value.get(1, Format::Text).unwrap_or_default(),
            attributes: value.get(2, Format::Text).unwrap_or_default(),
            parent_schema: value.get(3, Format::Text).unwrap_or_default(),
            parent_name: value.get(4, Format::Text).unwrap_or_default(),
        }
    }
}

/// Get replica identity for table. This has to be a unique index
/// or all columns in the table.
static REPLICA_IDENTIFY: &str = "SELECT
    c.oid,
    c.relreplident,
    c.relkind
FROM
    pg_catalog.pg_class c
INNER JOIN pg_catalog.pg_namespace n
ON (c.relnamespace = n.oid) WHERE n.nspname = $1 AND c.relname = $2";

/// Identifies the columns part of the replica identity for a table.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ReplicaIdentity {
    pub oid: Oid,
    pub identity: String,
    pub kind: String,
}

impl ReplicaIdentity {
    pub async fn load(table: &PublicationTable, server: &mut Server) -> Result<Self, Error> {
        let identity: ReplicaIdentity = server
            .fetch_all(
                REPLICA_IDENTIFY
                    .replace("$1", &quote_literal(&table.schema))
                    .replace("$2", &quote_literal(&table.name)),
            )
            .await?
            .pop()
            .ok_or(Error::NoReplicaIdentity(
                table.schema.clone(),
                table.name.clone(),
            ))?;
        Ok(identity)
    }
}

impl From<DataRow> for ReplicaIdentity {
    fn from(value: DataRow) -> Self {
        Self {
            oid: value.get(0, Format::Text).unwrap_or_default(),
            identity: value.get(1, Format::Text).unwrap_or_default(),
            kind: value.get(2, Format::Text).unwrap_or_default(),
        }
    }
}

/// Get columns for the table, with replica identity column(s) marked.
static COLUMNS: &str = "SELECT
    a.attnum,
    a.attname,
    a.atttypid,
    a.attnum = ANY(i.indkey)
FROM
    pg_catalog.pg_attribute a
    LEFT JOIN pg_catalog.pg_index i
    ON (i.indexrelid = pg_get_replica_identity_index($1))
    WHERE a.attnum > 0::pg_catalog.int2 AND NOT a.attisdropped AND a.attgenerated = '' AND a.attrelid = $2 ORDER BY a.attnum";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PublicationTableColumn {
    /// Column number (`pg_attribute.attnum`). Despite the name, this is not an OID.
    pub oid: i32,
    pub name: String,
    /// Type OID (`pg_attribute.atttypid`).
    pub type_oid: Oid,
    pub identity: bool,
}

impl PublicationTableColumn {
    pub async fn load(identity: &ReplicaIdentity, server: &mut Server) -> Result<Vec<Self>, Error> {
        Ok(server
            .fetch_all(
                COLUMNS
                    .replace("$1", &identity.oid.to_string())
                    .replace("$2", &identity.oid.to_string()),
            )
            .await?)
    }
}

impl From<DataRow> for PublicationTableColumn {
    fn from(value: DataRow) -> Self {
        Self {
            oid: value.get(0, Format::Text).unwrap_or_default(),
            name: value.get(1, Format::Text).unwrap_or_default(),
            type_oid: value.get(2, Format::Text).unwrap_or_default(),
            identity: value.get(3, Format::Text).unwrap_or_default(),
        }
    }
}

/// Returns the subset of `tables` that have no qualifying unique index on `server`.
///
/// A qualifying index must satisfy:
/// - `indisunique`, `indisvalid`, `indisready`, `indislive`: skip indexes mid-build or mid-drop.
/// - `indpred IS NULL`: skip partial indexes (predicate rows are not constrained).
/// - `indexprs IS NULL`: skip expression indexes (constraint is on computed values).
/// - NULL-safety: either `indnullsnotdistinct = true` (PG15+, NULLs treated as equal in the
///   unique constraint) or every indexed attribute has `attnotnull = true` (NULLs impossible).
///   A plain nullable unique index allows two NULL-keyed rows to coexist during the copy–stream
///   overlap window, leaving the destination with more rows than the source.
/// Requires PostgreSQL 15+ when an `indnullsnotdistinct` index is present; the column does
/// not exist on older servers and the query will return an error rather than silently accept.
///
/// If `tables` is empty, no query is issued and an empty vec is returned.
static UNIQUE_INDEX: &str = "SELECT DISTINCT n.nspname, c.relname
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE (n.nspname, c.relname) IN ($1)
  AND i.indisunique
  AND i.indisvalid
  AND i.indisready
  AND i.indislive
  AND i.indpred IS NULL
  AND i.indexprs IS NULL
  AND (
    i.indnullsnotdistinct
    OR NOT EXISTS (
      SELECT 1
      FROM unnest(i.indkey) AS k(attnum)
      JOIN pg_catalog.pg_attribute a
        ON a.attrelid = i.indrelid AND a.attnum = k.attnum
      WHERE NOT a.attnotnull
    )
  )";

pub async fn tables_missing_unique_index<'a>(
    tables: impl IntoIterator<Item = &'a PublicationTable>,
    server: &mut Server,
) -> Result<Vec<String>, Error> {
    let tables: Vec<&PublicationTable> = tables.into_iter().collect();

    // Build `(nspname, relname) IN (('schema1','table1'), ...)` as literal string substitution.
    // `fetch_all` uses the simple query protocol (no extended protocol on replication connections),
    // so we cannot use real bind parameters here.
    let in_list = tables
        .iter()
        .map(|t| {
            format!(
                "({}, {})",
                quote_literal(t.destination_schema()),
                quote_literal(t.destination_name())
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    if in_list.is_empty() {
        return Ok(Vec::new());
    }

    let rows: Vec<DataRow> = server
        .fetch_all(UNIQUE_INDEX.replace("$1", &in_list))
        .await?;
    let found: HashSet<(String, String)> = rows
        .into_iter()
        .map(|row| {
            Ok((
                row.get(0, Format::Text).ok_or(Error::MissingData)?,
                row.get(1, Format::Text).ok_or(Error::MissingData)?,
            ))
        })
        .collect::<Result<HashSet<_>, Error>>()?;

    Ok(tables
        .into_iter()
        .filter(|t| {
            !found.contains(&(
                t.destination_schema().to_string(),
                t.destination_name().to_string(),
            ))
        })
        .map(|t| t.to_string())
        .collect())
}

#[cfg(test)]
mod test {
    use crate::backend::server::test::test_server;

    use super::*;

    #[test]
    fn test_replica_identity_decodes_oid_above_i32_max() {
        // Regression for issue #847: pg_class.oid is unsigned 32-bit, and in
        // long-lived databases it routinely exceeds i32::MAX. Decoding such an
        // OID as i32 used to silently produce 0, which caused PgDog to send
        // queries against OID 0 and trigger Postgres' "could not open relation
        // with OID 0" error. With Oid (u32), this round-trips correctly.
        let mut row = DataRow::new();
        row.add(Oid(2_500_000_000))
            .add("d".to_string())
            .add("r".to_string());
        let identity = ReplicaIdentity::from(row);
        assert_eq!(identity.oid, Oid(2_500_000_000));
        assert_eq!(identity.identity, "d");
        assert_eq!(identity.kind, "r");
    }

    #[test]
    fn test_replica_identity_substitutes_high_oid_into_columns_query() {
        // The COLUMNS query embeds identity.oid as text via Display. Verify
        // that a high OID renders as an unsigned decimal, not a negative i32.
        let identity = ReplicaIdentity {
            oid: Oid(2_500_000_000),
            identity: "d".to_string(),
            kind: "r".to_string(),
        };
        let rendered = COLUMNS
            .replace("$1", &identity.oid.to_string())
            .replace("$2", &identity.oid.to_string());
        assert!(rendered.contains("pg_get_replica_identity_index(2500000000)"));
        assert!(rendered.contains("a.attrelid = 2500000000"));
        assert!(!rendered.contains("(0)"));
        assert!(!rendered.contains("= 0 "));
    }

    #[test]
    fn test_publication_table_column_decodes_high_type_oid() {
        // pg_attribute.atttypid is also of type oid; user-defined types in
        // long-lived databases can exceed i32::MAX.
        let mut row = DataRow::new();
        row.add(1_i64.to_string()) // attnum
            .add("col".to_string())
            .add(Oid(3_000_000_000)) // atttypid
            .add("t".to_string()); // identity bool
        let column = PublicationTableColumn::from(row);
        assert_eq!(column.type_oid, Oid(3_000_000_000));
        assert_eq!(column.name, "col");
        assert!(column.identity);
    }

    #[tokio::test]
    async fn test_oid_above_i32_max_round_trips_from_postgres() {
        // Regression for issue #847: ask Postgres to emit a real `oid` value
        // above i32::MAX on the wire and verify our text-format decode handles
        // it. With the previous i32-based parser this returned 0; with Oid(u32)
        // the value round-trips correctly.
        let mut server = test_server().await;
        let rows: Vec<DataRow> = server
            .fetch_all("SELECT 2500000000::oid, 4294967295::oid")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let high: Oid = rows[0].get(0, Format::Text).unwrap();
        let max: Oid = rows[0].get(1, Format::Text).unwrap();
        assert_eq!(high, Oid(2_500_000_000));
        assert_eq!(max, Oid(u32::MAX));
    }

    #[tokio::test]
    async fn test_logical_publisher_queries() {
        let mut server = test_server().await;

        server.execute("BEGIN").await.unwrap();
        server
            .execute(
                "CREATE TABLE
            users_logical_pub_queries (
                id BIGSERIAL PRIMARY KEY,
                email VARCHAR NOT NULL UNIQUE
            )",
            )
            .await
            .unwrap();
        server
            .execute(
                "CREATE TABLE users_logical_pub_profiles (
            id BIGINT PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users_logical_pub_queries(id)
        )",
            )
            .await
            .unwrap();
        server
            .execute(
                "CREATE PUBLICATION
            users_logical_pub_queries
            FOR TABLE users_logical_pub_queries, users_logical_pub_profiles",
            )
            .await
            .unwrap();

        let tables = PublicationTable::load("users_logical_pub_queries", &mut server)
            .await
            .unwrap();
        assert_eq!(tables.len(), 2);
        for table in tables {
            let identity = ReplicaIdentity::load(&table, &mut server).await.unwrap();
            let columns = PublicationTableColumn::load(&identity, &mut server)
                .await
                .unwrap();
            assert_eq!(columns.len(), 2);
        }
        server.execute("ROLLBACK").await.unwrap();
    }

    /// Table with no unique index: must appear in missing set.
    #[tokio::test]
    async fn test_has_unique_index_no_index() {
        let mut server = test_server().await;
        server.execute("BEGIN").await.unwrap();
        server
            .execute(
                "CREATE TABLE huidx_no_index (
                    a TEXT,
                    b INTEGER
                )",
            )
            .await
            .unwrap();
        let table = PublicationTable {
            schema: "pgdog".to_string(),
            name: "huidx_no_index".to_string(),
            ..Default::default()
        };
        let result = tables_missing_unique_index(std::iter::once(&table), &mut server)
            .await
            .unwrap();
        assert_eq!(
            result.len(),
            1,
            "expected missing: table has no unique index"
        );
        server.execute("ROLLBACK").await.unwrap();
    }

    /// Unique index on a NOT NULL column: must not appear in missing set.
    #[tokio::test]
    async fn test_has_unique_index_with_index() {
        let mut server = test_server().await;
        server.execute("BEGIN").await.unwrap();
        server
            .execute(
                "CREATE TABLE huidx_with_index (
                    a TEXT NOT NULL,
                    b INTEGER
                )",
            )
            .await
            .unwrap();
        server
            .execute("CREATE UNIQUE INDEX ON huidx_with_index (a)")
            .await
            .unwrap();
        let table = PublicationTable {
            schema: "pgdog".to_string(),
            name: "huidx_with_index".to_string(),
            ..Default::default()
        };
        let result = tables_missing_unique_index(std::iter::once(&table), &mut server)
            .await
            .unwrap();
        assert!(
            result.is_empty(),
            "expected not missing: NOT NULL unique key column is safe for ON CONFLICT dedup"
        );
        server.execute("ROLLBACK").await.unwrap();
    }

    /// Nullable unique index: must appear in missing set (NULLs are not distinct by default).
    /// PG unique indexes treat NULLs as distinct, so nullable columns allow duplicate NULL rows
    /// to be inserted during the copy–stream overlap window, corrupting the destination row count.
    #[tokio::test]
    async fn test_has_unique_index_rejects_nullable_unique_column() {
        let mut server = test_server().await;
        server.execute("BEGIN").await.unwrap();
        server
            .execute(
                "CREATE TABLE huidx_nullable (
                    a TEXT,
                    b INTEGER
                )",
            )
            .await
            .unwrap();
        server
            .execute("CREATE UNIQUE INDEX ON huidx_nullable (a)")
            .await
            .unwrap();
        let table = PublicationTable {
            schema: "pgdog".to_string(),
            name: "huidx_nullable".to_string(),
            ..Default::default()
        };
        let result = tables_missing_unique_index(std::iter::once(&table), &mut server)
            .await
            .unwrap();
        assert_eq!(
            result.len(),
            1,
            "nullable unique column does not enforce NULL-vs-NULL conflict; must be missing"
        );
        server.execute("ROLLBACK").await.unwrap();
    }

    /// `NULLS NOT DISTINCT` unique index on nullable column: must not appear in missing set (PG15+).
    /// `indnullsnotdistinct = true` makes NULLs compare as equal, so two NULL-keyed rows
    /// cannot coexist — the index is safe for `ON CONFLICT DO NOTHING` deduplication.
    #[tokio::test]
    async fn test_has_unique_index_accepts_nulls_not_distinct() {
        let mut server = test_server().await;
        server.execute("BEGIN").await.unwrap();
        server
            .execute(
                "CREATE TABLE huidx_nulls_not_distinct (
                    a TEXT,
                    b INTEGER
                )",
            )
            .await
            .unwrap();
        server
            .execute("CREATE UNIQUE INDEX ON huidx_nulls_not_distinct (a) NULLS NOT DISTINCT")
            .await
            .unwrap();
        let table = PublicationTable {
            schema: "pgdog".to_string(),
            name: "huidx_nulls_not_distinct".to_string(),
            ..Default::default()
        };
        let result = tables_missing_unique_index(std::iter::once(&table), &mut server)
            .await
            .unwrap();
        assert!(
            result.is_empty(),
            "NULLS NOT DISTINCT index prevents NULL-keyed duplicates; must not be missing"
        );
        server.execute("ROLLBACK").await.unwrap();
    }
}
