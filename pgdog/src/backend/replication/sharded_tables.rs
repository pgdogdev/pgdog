//! Tables sharded in the database.
use crate::{
    config::{DataType, ShardedTable},
    net::messages::Vector,
};
use std::{collections::HashSet, sync::Arc};

#[derive(Debug, Clone, Default)]
pub struct ShardedTables {
    tables: Arc<Vec<ShardedTable>>,
    omnisharded: Arc<HashSet<String>>,
    dry_run: bool,
}

impl From<&[ShardedTable]> for ShardedTables {
    fn from(value: &[ShardedTable]) -> Self {
        Self::new(value.to_vec(), vec![], false)
    }
}

impl ShardedTables {
    pub fn new(tables: Vec<ShardedTable>, omnisharded_tables: Vec<String>, dry_run: bool) -> Self {
        Self {
            tables: Arc::new(tables.to_vec()),
            omnisharded: Arc::new(omnisharded_tables.into_iter().collect()),
            dry_run,
        }
    }

    pub fn tables(&self) -> &[ShardedTable] {
        &self.tables
    }

    pub fn omnishards(&self) -> &HashSet<String> {
        &self.omnisharded
    }

    /// Find a specific sharded table.
    pub fn table(&self, name: &str) -> Option<&ShardedTable> {
        self.tables()
            .iter()
            .find(|t| t.name.as_deref() == Some(name))
    }

    /// Find out which column (if any) is sharded in the given table.
    pub fn sharded_column(&self, table: &str, columns: &[&str]) -> Option<ShardedColumn> {
        let with_names = self
            .tables()
            .iter()
            .filter(|t| t.name.is_some())
            .collect::<Vec<_>>();
        let without_names = self.tables().iter().filter(|t| t.name.is_none());

        let get_column = |sharded_table: &ShardedTable, columns: &[&str]| {
            columns
                .iter()
                .position(|c| *c == sharded_table.column)
                .map(|position| ShardedColumn {
                    data_type: sharded_table.data_type,
                    position,
                    centroids: sharded_table.centroids.clone(),
                    centroid_probes: sharded_table.centroid_probes,
                })
        };

        for sharded_table in with_names {
            if Some(table) == sharded_table.name.as_deref() {
                if let Some(column) = get_column(sharded_table, columns) {
                    return Some(column);
                }
            }
        }

        for sharded_table in without_names {
            if let Some(column) = get_column(sharded_table, columns) {
                return Some(column);
            }
        }

        None
    }

    pub(crate) fn dry_run(&self) -> bool {
        self.dry_run
    }
}

#[derive(Debug, Clone)]
pub struct ShardedColumn {
    pub data_type: DataType,
    pub position: usize,
    pub centroids: Vec<Vector>,
    pub centroid_probes: usize,
}

impl ShardedColumn {
    pub fn from_sharded_table(table: &ShardedTable, columns: &[&str]) -> Option<Self> {
        columns
            .iter()
            .position(|c| *c == table.column.as_str())
            .map(|index| ShardedColumn {
                data_type: table.data_type,
                position: index,
                centroids: table.centroids.clone(),
                centroid_probes: table.centroid_probes,
            })
    }
}
