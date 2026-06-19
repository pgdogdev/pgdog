use pgdog_config::{DataType, Hasher};
use pgdog_vector::Vector;

use crate::{
    backend::ShardingSchema,
    frontend::router::parser::{Column, Table},
};

use super::Mapping;

/// Runtime representation of a sharded table, derived from [`pgdog_config::ShardedTableConfig`].
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ShardedTable {
    pub database: String,
    pub name: Option<String>,
    pub schema: Option<String>,
    pub column: String,
    pub primary: bool,
    pub centroids: Vec<Vector>,
    pub data_type: DataType,
    pub centroid_probes: usize,
    pub hasher: Hasher,
    pub mapping: Option<Mapping>,
}

#[derive(Debug)]
pub struct Key<'a> {
    pub table: &'a ShardedTable,
    pub position: usize,
}

pub struct Tables<'a> {
    schema: &'a ShardingSchema,
}

impl<'a> Tables<'a> {
    pub(crate) fn new(schema: &'a ShardingSchema) -> Self {
        Tables { schema }
    }

    pub(crate) fn sharded(&'a self, table: Table) -> Option<&'a ShardedTable> {
        let tables = self.schema.tables().tables();

        tables
            .iter()
            .filter(|table| table.name.is_some())
            .find(|t| t.name.as_deref() == Some(table.name))
    }

    pub(crate) fn key(&'a self, table: Table, columns: &'a [Column]) -> Option<Key<'a>> {
        let tables = self.schema.tables().tables();

        // Check tables with name first.
        let sharded = tables
            .iter()
            .filter(|table| table.name.is_some())
            .find(|t| t.name.as_deref() == Some(table.name));

        if let Some(sharded) = sharded
            && let Some(position) = columns.iter().position(|col| col.name == sharded.column)
        {
            return Some(Key {
                table: sharded,
                position,
            });
        }

        // Check tables without name.
        let key: Option<(&'a ShardedTable, Option<usize>)> = tables
            .iter()
            .filter(|table| table.name.is_none())
            .map(|t| (t, columns.iter().position(|col| col.name == t.column)))
            .find(|t| t.1.is_some());
        if let Some(key) = key
            && let Some(position) = key.1
        {
            return Some(Key {
                table: key.0,
                position,
            });
        }

        None
    }
}
