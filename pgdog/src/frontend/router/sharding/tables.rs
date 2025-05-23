use crate::{
    backend::ShardingSchema,
    frontend::router::parser::{Column, Table},
};

pub struct Tables<'a> {
    schema: &'a ShardingSchema,
}

impl<'a> Tables<'a> {
    pub(crate) fn new(schema: &'a ShardingSchema) -> Self {
        Tables { schema }
    }

    pub(crate) fn key(&self, table: Table, columns: &[Column]) -> Option<usize> {
        let tables = self.schema.tables().tables();

        // Check tables with name first.
        let sharded = tables
            .iter()
            .filter(|table| table.name.is_some())
            .find(|t| t.name.as_ref().map(|s| s.as_str()) == Some(table.name));

        if let Some(sharded) = sharded {
            return columns.iter().position(|col| col.name == sharded.column);
        }

        // Check tables without name.
        let key = tables
            .iter()
            .filter(|table| table.name.is_none())
            .map(|t| columns.iter().position(|col| col.name == t.column))
            .filter(|p| p.is_some())
            .next()
            .flatten();

        key
    }
}
