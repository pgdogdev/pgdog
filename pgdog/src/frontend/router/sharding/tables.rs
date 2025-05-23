use crate::{
    backend::ShardingSchema,
    config::ShardedTable,
    frontend::router::parser::{Column, Table},
};

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

        let sharded = tables
            .iter()
            .filter(|table| table.name.is_some())
            .find(|t| t.name.as_ref().map(|s| s.as_str()) == Some(table.name));

        sharded
    }

    pub(crate) fn key(&'a self, table: Table, columns: &'a [Column]) -> Option<Key<'a>> {
        let tables = self.schema.tables().tables();

        // Check tables with name first.
        let sharded = tables
            .iter()
            .filter(|table| table.name.is_some())
            .find(|t| t.name.as_ref().map(|s| s.as_str()) == Some(table.name));

        if let Some(sharded) = sharded {
            if let Some(position) = columns.iter().position(|col| col.name == sharded.column) {
                return Some(Key {
                    table: sharded,
                    position,
                });
            }
        }

        // Check tables without name.
        let key: Option<(&'a ShardedTable, Option<usize>)> = tables
            .iter()
            .filter(|table| table.name.is_none())
            .map(|t| (t, columns.iter().position(|col| col.name == t.column)))
            .filter(|t| t.1.is_some())
            .next();
        if let Some(key) = key {
            if let Some(position) = key.1 {
                return Some(Key {
                    table: key.0,
                    position,
                });
            }
        }

        None
    }
}
