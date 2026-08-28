use pgdog_config::{DataType, Hasher, LookupResult};
use pgdog_vector::Vector;

use crate::{
    backend::ShardingSchema,
    frontend::router::parser::{Column, Table},
};

use super::Mapping;

/// Runtime representation of a sharded table, derived from [`pgdog_config::ShardedTableConfig`].
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct ShardedTable {
    pub(crate) database: String,
    pub(crate) name: Option<String>,
    pub(crate) schema: Option<String>,
    pub(crate) column: String,
    pub(crate) primary: bool,
    pub(crate) centroids: Vec<Vector>,
    pub(crate) data_type: DataType,
    pub(crate) centroid_probes: usize,
    pub(crate) hasher: Hasher,
    pub(crate) mapping: Option<Mapping>,
    pub(crate) lookup_query: Option<String>,
    pub(crate) lookup_result: LookupResult,
}

#[derive(Debug)]
pub(crate) struct Key<'a> {
    pub(crate) table: &'a ShardedTable,
    pub(crate) position: usize,
}

pub(crate) struct Tables<'a> {
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
