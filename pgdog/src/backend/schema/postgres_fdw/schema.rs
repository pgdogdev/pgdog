//! Foreign table schema query and data structures.

use std::collections::{HashMap, HashSet};
use tracing::debug;

use crate::{
    backend::{schema::postgres_fdw::create_foreign_table, Server, ShardingSchema},
    net::messages::DataRow,
};

use super::custom_types::CustomTypes;
use super::extensions::Extensions;

/// Query to fetch table and column information needed for CREATE FOREIGN TABLE statements.
pub static FOREIGN_TABLE_SCHEMA: &str = include_str!("postgres_fdw.sql");

/// Row from the foreign table schema query.
/// Each row represents a single column in a table, or a table with no columns.
#[derive(Debug, Clone)]
pub struct ForeignTableColumn {
    pub schema_name: String,
    pub table_name: String,
    /// Empty if the table has no columns.
    pub column_name: String,
    /// Column type with modifiers, e.g. "character varying(255)".
    pub column_type: String,
    pub is_not_null: bool,
    /// Default expression, also used for generated column expressions.
    pub column_default: String,
    /// 's' for stored generated column, empty otherwise.
    pub generated: String,
    pub collation_name: String,
    pub collation_schema: String,
}

impl From<DataRow> for ForeignTableColumn {
    fn from(value: DataRow) -> Self {
        Self {
            schema_name: value.get_text(0).unwrap_or_default(),
            table_name: value.get_text(1).unwrap_or_default(),
            column_name: value.get_text(2).unwrap_or_default(),
            column_type: value.get_text(3).unwrap_or_default(),
            is_not_null: value.get_text(4).unwrap_or_default() == "true",
            column_default: value.get_text(5).unwrap_or_default(),
            generated: value.get_text(6).unwrap_or_default(),
            collation_name: value.get_text(7).unwrap_or_default(),
            collation_schema: value.get_text(8).unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ForeignTableSchema {
    tables: HashMap<(String, String), Vec<ForeignTableColumn>>,
    extensions: Extensions,
    custom_types: CustomTypes,
}

impl ForeignTableSchema {
    pub(crate) async fn load(server: &mut Server) -> Result<Self, super::super::Error> {
        let tables = ForeignTableColumn::load(server).await?;
        let extensions = Extensions::load(server).await?;
        let custom_types = CustomTypes::load(server).await?;
        Ok(Self {
            tables,
            extensions,
            custom_types,
        })
    }

    pub(crate) async fn setup(
        &self,
        server: &mut Server,
        sharding_schema: &ShardingSchema,
    ) -> Result<(), super::super::Error> {
        // Create extensions first (types may depend on them)
        self.extensions.setup(server).await?;

        // Create custom types (enums, domains, composite types)
        self.custom_types.setup(server).await?;

        let mut schemas = HashSet::new();
        let mut tables = HashSet::new();

        for ((schema, table), columns) in &self.tables {
            if !schemas.contains(schema) {
                server
                    .execute(&format!(
                        "CREATE SCHEMA IF NOT EXISTS {}",
                        super::quote_identifier(&schema)
                    ))
                    .await?;
                schemas.insert(schema.clone());
            }

            let dedup = (schema.clone(), table.clone());
            if !tables.contains(&dedup) {
                let statements = create_foreign_table(columns, sharding_schema)?;
                for sql in statements {
                    debug!("[fdw::setup] {} [{}]", sql, server.addr());
                    server.execute(&sql).await?;
                }
                tables.insert(dedup);
            }
        }
        Ok(())
    }

    /// Get the extensions.
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// Get the custom types.
    pub fn custom_types(&self) -> &CustomTypes {
        &self.custom_types
    }
}

impl ForeignTableColumn {
    /// Check if this column has a collation.
    pub(super) fn has_collation(&self) -> bool {
        !self.collation_name.is_empty() && !self.collation_schema.is_empty()
    }

    /// Fetch columns and organize by schema and table name.
    async fn load(
        server: &mut Server,
    ) -> Result<HashMap<(String, String), Vec<Self>>, crate::backend::Error> {
        let mut result = HashMap::new();
        let rows: Vec<Self> = server.fetch_all(FOREIGN_TABLE_SCHEMA).await?;

        for row in rows {
            let entry = result
                .entry((row.schema_name.clone(), row.table_name.clone()))
                .or_insert_with(Vec::new);
            entry.push(row);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::backend::server::test::test_server;

    #[tokio::test]
    async fn test_load_foreign_table_schema() {
        let mut server = test_server().await;

        server
            .execute("DROP TABLE IF EXISTS test_fdw_schema")
            .await
            .unwrap();

        server
            .execute(
                "CREATE TABLE test_fdw_schema (
                    id BIGINT NOT NULL,
                    name VARCHAR(100) DEFAULT 'unknown',
                    score NUMERIC(10, 2),
                    created_at TIMESTAMP NOT NULL DEFAULT now()
                )",
            )
            .await
            .unwrap();

        let schema = ForeignTableSchema::load(&mut server).await.unwrap();
        let rows: Vec<_> = schema.tables.into_values().flatten().collect();

        assert!(!rows.is_empty());

        let test_rows: Vec<_> = rows
            .iter()
            .filter(|r| r.table_name == "test_fdw_schema")
            .collect();
        assert_eq!(test_rows.len(), 4);

        let id_col = test_rows.iter().find(|r| r.column_name == "id").unwrap();
        assert!(id_col.is_not_null);
        assert!(id_col.column_default.is_empty());

        let name_col = test_rows.iter().find(|r| r.column_name == "name").unwrap();
        assert!(!name_col.is_not_null);
        assert!(!name_col.column_default.is_empty());

        server
            .execute("DROP TABLE IF EXISTS test_fdw_schema")
            .await
            .unwrap();
    }
}
