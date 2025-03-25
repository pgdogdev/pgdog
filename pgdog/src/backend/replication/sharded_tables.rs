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
        let mut tables = self
            .tables()
            .iter()
            .filter(|t| t.name.is_some())
            .collect::<Vec<_>>();
        tables.extend(self.tables().iter().filter(|t| t.name.is_none()));
        for sharded_table in tables {
            if Some(table) == sharded_table.name.as_deref() {
                if let Some(position) = columns.iter().position(|c| *c == sharded_table.column) {
                    return Some(ShardedColumn {
                        data_type: sharded_table.data_type,
                        position,
                        centroids: sharded_table.centroids.clone(),
                    });
                }
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
