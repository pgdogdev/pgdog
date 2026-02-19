use crate::{backend::ShardingSchema, config::ShardedTable, frontend::router::parser::Table};

pub struct Tables<'a> {
    schema: &'a ShardingSchema,
}

impl<'a> Tables<'a> {
    pub(crate) fn new(schema: &'a ShardingSchema) -> Self {
        Tables { schema }
    }

    pub(crate) fn sharded(&'a self, table: Table) -> Option<&'a ShardedTable> {
        self.schema
            .tables()
            .tables()
            .iter()
            .filter(|table| table.name.is_some())
            .find(|t| t.name.as_deref() == Some(table.name))
    }
}
