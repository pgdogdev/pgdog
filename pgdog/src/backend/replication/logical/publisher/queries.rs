//! Queries to fetch publication info.
//!
//! TODO: I think these are Postgres-version specific, so we need to handle that
//! later. These were fetched from CREATE SUBSCRIPTION ran on Postgres 17.
//!
use std::fmt::Display;

use pgdog_postgres_types::Oid;

use crate::{
    backend::Server,
    net::{DataRow, Format},
};

use super::super::Error;

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
        Ok(server
            .fetch_all(TABLES.replace("$1", &format!("'{}'", publication)))
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
                    .replace("$1", &format!("'{}'", &table.schema))
                    .replace("$2", &format!("'{}'", &table.name)),
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
                    .replace("$1", &identity.oid.to_string()) // Don't feel like using prepared statements.
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
}
