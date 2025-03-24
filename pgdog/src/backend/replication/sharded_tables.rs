//! Tables sharded in the database.
use crate::{
    config::{DataType, ShardedTable},
    net::messages::Vector,
};

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

    /// Find out which column (if any) is sharded in the given table.
    pub fn sharded_column(&self, table: &str, columns: &[&str]) -> Option<ShardedColumn> {
        let table = self.tables.iter().find(|sharded_table| {
            sharded_table
                .name
                .as_ref()
                .map(|name| name == table)
                .unwrap_or(true)
                && columns.contains(&sharded_table.column.as_str())
        });

        if let Some(table) = table {
            let position = columns.iter().position(|c| *c == table.column);
            if let Some(position) = position {
                return Some(ShardedColumn {
                    data_type: table.data_type,
                    position,
                    centroids: table.centroids.clone(),
                });
            }
        }

        None
    }
}

#[derive(Debug, Clone)]
pub struct ShardedColumn {
    pub data_type: DataType,
    pub position: usize,
    pub centroids: Vec<Vector>,
}
