//! Test helpers for creating schema structures.
//!
//! These helpers are used across multiple test modules to create
//! schemas with tables, columns, and foreign key relationships.

use std::collections::HashMap;

use indexmap::IndexMap;
use pgdog_stats::{Column as StatsColumn, ForeignKey, Relation as StatsRelation};

use super::relation::Relation;
use super::Schema;
use crate::backend::pool::cluster::ShardingSchema;
use crate::backend::replication::sharded_tables::ShardedTables;
use crate::config::ShardedTable;
use pgdog_config::SystemCatalogsBehavior;

/// Builder for creating test columns.
#[derive(Default)]
pub struct ColumnBuilder {
    schema: String,
    table: String,
    name: String,
    ordinal: i32,
    is_pk: bool,
    data_type: String,
    foreign_keys: Vec<ForeignKey>,
}

impl ColumnBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            data_type: "bigint".into(),
            ..Default::default()
        }
    }

    pub fn schema(mut self, schema: &str) -> Self {
        self.schema = schema.into();
        self
    }

    pub fn table(mut self, table: &str) -> Self {
        self.table = table.into();
        self
    }

    pub fn ordinal(mut self, ordinal: i32) -> Self {
        self.ordinal = ordinal;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.is_pk = true;
        self
    }

    pub fn data_type(mut self, data_type: &str) -> Self {
        self.data_type = data_type.into();
        self
    }

    pub fn foreign_key(mut self, ref_schema: &str, ref_table: &str, ref_column: &str) -> Self {
        self.foreign_keys.push(ForeignKey {
            schema: ref_schema.into(),
            table: ref_table.into(),
            column: ref_column.into(),
            ..Default::default()
        });
        self
    }

    pub fn build(self) -> StatsColumn {
        StatsColumn {
            table_catalog: "test".into(),
            table_schema: self.schema,
            table_name: self.table,
            column_name: self.name,
            column_default: String::new(),
            is_nullable: !self.is_pk,
            data_type: self.data_type,
            ordinal_position: self.ordinal,
            is_primary_key: self.is_pk,
            foreign_keys: self.foreign_keys,
        }
    }
}

/// Builder for creating test relations (tables).
pub struct RelationBuilder {
    schema: String,
    name: String,
    oid: i32,
    columns: Vec<StatsColumn>,
}

impl RelationBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            schema: "public".into(),
            name: name.into(),
            oid: 0,
            columns: vec![],
        }
    }

    pub fn schema(mut self, schema: &str) -> Self {
        self.schema = schema.into();
        self
    }

    pub fn oid(mut self, oid: i32) -> Self {
        self.oid = oid;
        self
    }

    pub fn column(mut self, col: ColumnBuilder) -> Self {
        let ordinal = self.columns.len() as i32 + 1;
        self.columns.push(
            col.schema(&self.schema)
                .table(&self.name)
                .ordinal(ordinal)
                .build(),
        );
        self
    }

    pub fn build(self) -> Relation {
        let cols: IndexMap<String, StatsColumn> = self
            .columns
            .into_iter()
            .map(|c| (c.column_name.clone(), c))
            .collect();
        StatsRelation {
            schema: self.schema,
            name: self.name,
            type_: "table".into(),
            owner: String::new(),
            persistence: String::new(),
            access_method: String::new(),
            description: String::new(),
            oid: self.oid,
            columns: cols,
            is_sharded: false,
        }
        .into()
    }
}

/// Builder for creating test schemas with multiple tables.
pub struct SchemaBuilder {
    search_path: Vec<String>,
    relations: HashMap<(String, String), Relation>,
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self {
            search_path: vec!["public".into()],
            relations: HashMap::new(),
        }
    }

    pub fn search_path(mut self, path: Vec<&str>) -> Self {
        self.search_path = path.into_iter().map(String::from).collect();
        self
    }

    pub fn relation(mut self, rel: RelationBuilder) -> Self {
        let relation = rel.build();
        let key = (relation.schema.clone(), relation.name.clone());
        self.relations.insert(key, relation);
        self
    }

    pub fn build(self) -> Schema {
        Schema::from_parts(self.search_path, self.relations)
    }
}

/// Builder for creating sharding configuration.
pub struct ShardingBuilder {
    shards: usize,
    tables: Vec<ShardedTable>,
}

impl Default for ShardingBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardingBuilder {
    pub fn new() -> Self {
        Self {
            shards: 2,
            tables: vec![],
        }
    }

    pub fn shards(mut self, n: usize) -> Self {
        self.shards = n;
        self
    }

    pub fn sharded_table(mut self, table: &str, column: &str) -> Self {
        self.tables.push(ShardedTable {
            database: "test".into(),
            name: Some(table.into()),
            column: column.into(),
            ..Default::default()
        });
        self
    }

    pub fn sharded_column(mut self, column: &str) -> Self {
        self.tables.push(ShardedTable {
            database: "test".into(),
            column: column.into(),
            ..Default::default()
        });
        self
    }

    pub fn database(mut self, database: &str) -> Self {
        if let Some(table) = self.tables.last_mut() {
            table.database = database.into();
        }
        self
    }

    pub fn build(self) -> ShardingSchema {
        ShardingSchema {
            shards: self.shards,
            tables: ShardedTables::new(
                self.tables,
                vec![],
                false,
                SystemCatalogsBehavior::default(),
            ),
            ..Default::default()
        }
    }
}

/// Convenience functions for common test patterns.
pub mod prelude {
    pub use super::{ColumnBuilder, RelationBuilder, SchemaBuilder, ShardingBuilder};

    /// Create a simple column.
    pub fn col(name: &str) -> ColumnBuilder {
        ColumnBuilder::new(name)
    }

    /// Create a primary key column.
    pub fn pk(name: &str) -> ColumnBuilder {
        ColumnBuilder::new(name).primary_key()
    }

    /// Create a foreign key column.
    pub fn fk(name: &str, ref_table: &str, ref_column: &str) -> ColumnBuilder {
        ColumnBuilder::new(name).foreign_key("public", ref_table, ref_column)
    }

    /// Create a table builder.
    pub fn table(name: &str) -> RelationBuilder {
        RelationBuilder::new(name)
    }

    /// Create a schema builder.
    pub fn schema() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    /// Create a sharding config builder.
    pub fn sharding() -> ShardingBuilder {
        ShardingBuilder::new()
    }
}

#[cfg(test)]
mod tests {
    use super::prelude::*;

    #[test]
    fn test_builders() {
        let db_schema = schema()
            .relation(
                table("users")
                    .oid(1001)
                    .column(pk("id"))
                    .column(col("user_id")),
            )
            .relation(
                table("orders")
                    .oid(1002)
                    .column(pk("id"))
                    .column(fk("user_id", "users", "id")),
            )
            .build();

        let sharding = sharding().sharded_table("users", "user_id").build();

        assert_eq!(db_schema.tables().len(), 2);
        assert_eq!(sharding.shards, 2);
    }
}
