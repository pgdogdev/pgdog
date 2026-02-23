//! Foreign table schema query and data structures.

use std::collections::{HashMap, HashSet};
use tracing::{debug, warn};

use crate::{
    backend::{
        schema::postgres_fdw::{create_foreign_table, create_foreign_table_with_children},
        Server, ShardingSchema,
    },
    net::messages::DataRow,
};

use super::custom_types::CustomTypes;
use super::extensions::Extensions;
use super::quote_identifier;
use super::TypeMismatch;

/// Server definition for FDW setup.
#[derive(Debug, Clone)]
pub struct FdwServerDef {
    pub shard_num: usize,
    pub host: String,
    pub port: u16,
    pub database_name: String,
    pub user: String,
    pub password: String,
    pub mapping_user: String,
}

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
    /// Whether this table is a partition of another table.
    pub is_partition: bool,
    /// Parent table name if this is a partition, empty otherwise.
    pub parent_table_name: String,
    /// Parent schema name if this is a partition, empty otherwise.
    pub parent_schema_name: String,
    /// Partition bound expression, e.g. "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')".
    pub partition_bound: String,
    /// Partition key definition if this is a partitioned table, e.g. "RANGE (created_at)".
    pub partition_key: String,
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
            is_partition: value.get_text(9).unwrap_or_default() == "true",
            parent_table_name: value.get_text(10).unwrap_or_default(),
            parent_schema_name: value.get_text(11).unwrap_or_default(),
            partition_bound: value.get_text(12).unwrap_or_default(),
            partition_key: value.get_text(13).unwrap_or_default(),
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

    /// Full setup: creates servers, schemas, types, and tables.
    pub(crate) async fn setup(
        &self,
        server: &mut Server,
        sharding_schema: &ShardingSchema,
        servers: &[FdwServerDef],
    ) -> Result<(), super::super::Error> {
        server.execute("BEGIN").await?;

        // Drop and recreate managed schemas (CASCADE drops tables and types).
        // This also drops any extensions installed in those schemas.
        self.drop_schemas(server).await?;

        // Drop and recreate servers (must happen after schema drop, before foreign table creation)
        for srv in servers {
            server
                .execute(format!(
                    r#"DROP SERVER IF EXISTS "shard_{}" CASCADE"#,
                    srv.shard_num
                ))
                .await?;

            server
                .execute(format!(
                    r#"CREATE SERVER "shard_{}"
                        FOREIGN DATA WRAPPER postgres_fdw
                        OPTIONS (host '{}', port '{}', dbname '{}')"#,
                    srv.shard_num, srv.host, srv.port, srv.database_name,
                ))
                .await?;

            server
                .execute(format!(
                    r#"CREATE USER MAPPING
                        FOR {}
                        SERVER "shard_{}"
                        OPTIONS (user '{}', password '{}')"#,
                    quote_identifier(&srv.mapping_user),
                    srv.shard_num,
                    srv.user,
                    srv.password,
                ))
                .await?;
        }

        self.create_schemas(server).await?;

        // Install extensions after schemas are recreated so CASCADE
        // doesn't destroy them.
        self.extensions.setup(server).await?;

        // Create custom types (enums, domains, composite types)
        self.custom_types.setup(server).await?;

        // Build a map of parent tables to their child partitions
        let children_map = self.build_children_map();

        let mut processed_tables = HashSet::new();
        let mut all_type_mismatches: Vec<TypeMismatch> = Vec::new();

        for ((schema, table), columns) in &self.tables {
            // Skip internal PgDog tables
            if Self::is_internal_table(schema, table) {
                continue;
            }

            // Skip partitions - they are handled when processing their parent
            if columns.first().is_some_and(|c| c.is_partition) {
                continue;
            }

            let dedup = (schema.clone(), table.clone());
            if !processed_tables.contains(&dedup) {
                // Check if this table has child partitions
                let children = children_map
                    .get(&dedup)
                    .map(|child_keys| {
                        child_keys
                            .iter()
                            .filter_map(|key| self.tables.get(key).cloned())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                let result = if children.is_empty() {
                    create_foreign_table(columns, sharding_schema)?
                } else {
                    create_foreign_table_with_children(columns, sharding_schema, children)?
                };

                for sql in &result.statements {
                    debug!("[fdw::setup] {} [{}]", sql, server.addr());
                    server.execute(sql).await?;
                }
                all_type_mismatches.extend(result.type_mismatches);
                processed_tables.insert(dedup);
            }
        }

        // Log summary of type mismatches if any were found
        if !all_type_mismatches.is_empty() {
            warn!(
                "[fdw] {} table(s) skipped due to sharding config type mismatches:",
                all_type_mismatches.len()
            );
            for mismatch in &all_type_mismatches {
                warn!("[fdw]   - {}", mismatch);
            }
        }

        server.execute("COMMIT").await?;
        Ok(())
    }

    /// Add user mappings only (for additional users on an already-configured database).
    pub(crate) async fn setup_user_mappings(
        server: &mut Server,
        servers: &[FdwServerDef],
    ) -> Result<(), super::super::Error> {
        for srv in servers {
            server
                .execute(format!(
                    r#"CREATE USER MAPPING IF NOT EXISTS
                        FOR {}
                        SERVER "shard_{}"
                        OPTIONS (user '{}', password '{}')"#,
                    quote_identifier(&srv.mapping_user),
                    srv.shard_num,
                    srv.user,
                    srv.password,
                ))
                .await?;
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

    /// Get the tables map (for testing).
    #[cfg(test)]
    pub fn tables(&self) -> &HashMap<(String, String), Vec<ForeignTableColumn>> {
        &self.tables
    }

    /// Check if a table is an internal PgDog table that shouldn't be exposed via FDW.
    fn is_internal_table(schema: &str, table: &str) -> bool {
        schema == "pgdog" && matches!(table, "validator_bigint" | "validator_uuid" | "config")
    }

    /// Build a map of parent tables to their child partition keys.
    fn build_children_map(&self) -> HashMap<(String, String), Vec<(String, String)>> {
        let mut children_map: HashMap<(String, String), Vec<(String, String)>> = HashMap::new();

        for ((schema, table), columns) in &self.tables {
            if let Some(first_col) = columns.first() {
                if first_col.is_partition && !first_col.parent_table_name.is_empty() {
                    let parent_key = (
                        first_col.parent_schema_name.clone(),
                        first_col.parent_table_name.clone(),
                    );
                    let child_key = (schema.clone(), table.clone());
                    children_map.entry(parent_key).or_default().push(child_key);
                }
            }
        }

        children_map
    }

    /// Collect unique schemas from tables and custom types.
    fn schemas(&self) -> HashSet<String> {
        self.tables
            .keys()
            .map(|(s, _)| s.clone())
            .chain(
                self.custom_types
                    .types()
                    .iter()
                    .map(|t| t.schema_name.clone()),
            )
            .collect()
    }

    /// Drop schemas we manage (with CASCADE to drop tables and types).
    async fn drop_schemas(&self, server: &mut Server) -> Result<(), super::super::Error> {
        for schema in &self.schemas() {
            server
                .execute(&format!(
                    "DROP SCHEMA IF EXISTS {} CASCADE",
                    super::quote_identifier(schema)
                ))
                .await?;
        }
        Ok(())
    }

    /// Create schemas we manage.
    async fn create_schemas(&self, server: &mut Server) -> Result<(), super::super::Error> {
        for schema in &self.schemas() {
            server
                .execute(&format!(
                    "CREATE SCHEMA {}",
                    super::quote_identifier(schema)
                ))
                .await?;
        }
        Ok(())
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
