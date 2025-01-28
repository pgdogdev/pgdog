use std::collections::HashMap;

use crate::config::ShardedTable;

#[derive(Debug, Clone, Default)]
pub struct ShardedTables {
    tables: Vec<ShardedTable>,
}

impl From<&[ShardedTable]> for ShardedTables {
    fn from(value: &[ShardedTable]) -> Self {
        Self::new(value.to_vec())
    }
}

impl ShardedTables {
    pub fn new(tables: Vec<ShardedTable>) -> Self {
        Self { tables }
    }

    pub fn tables(&self) -> &[ShardedTable] {
        &self.tables
    }

    pub fn sharded_column(&self, table: &str, columns: &[&str]) -> Option<usize> {
        let table = self.tables.iter().find(|sharded_table| {
            sharded_table
                .name
                .as_ref()
                .map(|name| name == table)
                .unwrap_or(true)
                && columns.contains(&sharded_table.column.as_str())
        });

        table.and_then(|t| columns.iter().position(|c| *c == t.column))
    }
}
