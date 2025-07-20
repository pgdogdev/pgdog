use crate::net::{DataRow, Format};

/// Get list of tables in publication.
static TABLES: &'static str = "SELECT DISTINCT n.nspname, c.relname, gpt.attrs
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN ( SELECT (pg_get_publication_tables(VARIADIC array_agg(pubname::text))).*
       FROM pg_publication
       WHERE pubname IN ($1)) AS gpt
    ON gpt.relid = c.oid";

/// Table included in a publication.
#[derive(Debug, Clone)]
pub struct PublicationTable {
    pub schema: String,
    pub name: String,
    pub attributes: String,
}

impl From<DataRow> for PublicationTable {
    fn from(value: DataRow) -> Self {
        Self {
            schema: value.get(0, Format::Text).unwrap_or_default(),
            name: value.get(1, Format::Text).unwrap_or_default(),
            attributes: value.get(2, Format::Text).unwrap_or_default(),
        }
    }
}

/// Get replica identity for table. This has to be a unique index
/// or all columns in the table.
static REPLICA_IDENTIFY: &'static str = "SELECT
    c.oid,
    c.relreplident,
    c.relkind
FROM
    pg_catalog.pg_class c
INNER JOIN pg_catalog.pg_namespace n
ON (c.relnamespace = n.oid) WHERE n.nspname = 'public' AND c.relname = $1";

/// Identifies the columns part of the replica identity for a table.
#[derive(Debug, Clone)]
pub struct ReplicaIdentity {
    pub oid: i32,
    pub identity: i32,
    pub kind: i32,
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
static COLUMNS: &'static str = "SELECT
    a.attnum,
    a.attname,
    a.atttypid,
    a.attnum = ANY(i.indkey)
FROM
    pg_catalog.pg_attribute a
    LEFT JOIN pg_catalog.pg_index i
    ON (i.indexrelid = pg_get_replica_identity_index($1))
    WHERE a.attnum > 0::pg_catalog.int2 AND NOT a.attisdropped AND a.attgenerated = '' AND a.attrelid = $2 ORDER BY a.attnum";

#[derive(Debug, Clone)]
pub struct PublicationTableColumn {
    pub oid: i32,
    pub name: String,
    pub type_oid: i32,
    pub number: i32,
}

impl From<DataRow> for PublicationTableColumn {
    fn from(value: DataRow) -> Self {
        Self {
            oid: value.get(0, Format::Text).unwrap_or_default(),
            name: value.get(1, Format::Text).unwrap_or_default(),
            type_oid: value.get(2, Format::Text).unwrap_or_default(),
            number: value.get(3, Format::Text).unwrap_or_default(),
        }
    }
}
