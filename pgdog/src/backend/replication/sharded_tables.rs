//! Tables sharded in the database.
use crate::{
    config::{DataType, ShardedTable},
    frontend::router::sharding::Mapping,
    net::messages::Vector,
};
use std::{collections::HashSet, sync::Arc};

#[derive(Default, Debug)]
struct Inner {
    tables: Vec<ShardedTable>,
    omnisharded: HashSet<String>,
    /// This is set only if we have the same sharding scheme
    /// across all tables, i.e., 3 tables with the same data type
    /// and list/range/hash function.
    common_mapping: Option<CommonMapping>,
}

#[derive(Debug)]
pub(crate) struct CommonMapping {
    /// The column data type.
    pub(crate) data_type: DataType,
    /// The list/range mapping, if any.
    /// If none, the column is using hash sharding.
    pub(crate) mapping: Option<Mapping>,
}

/// Sharded tables.
#[derive(Debug, Clone)]
pub(crate) struct ShardedTables {
    inner: Arc<Inner>,
}

impl Default for ShardedTables {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }
}

impl From<&[ShardedTable]> for ShardedTables {
    fn from(value: &[ShardedTable]) -> Self {
        Self::new(value.to_vec(), vec![])
    }
}

impl ShardedTables {
    pub(crate) fn new(tables: Vec<ShardedTable>, omnisharded_tables: Vec<String>) -> Self {
        let mut common_mapping = HashSet::new();
        for table in &tables {
            common_mapping.insert((
                table.data_type,
                table.mapping.clone(),
                table.centroid_probes,
                table.centroids.clone(),
            ));
        }

        let common_mapping = if common_mapping.len() == 1 {
            common_mapping.iter().next().map(|dt| CommonMapping {
                data_type: dt.0,
                mapping: dt.1.clone(),
            })
        } else {
            None
        };

        Self {
            inner: Arc::new(Inner {
                tables,
                omnisharded: omnisharded_tables.into_iter().collect(),
                common_mapping,
            }),
        }
    }

    pub(crate) fn tables(&self) -> &[ShardedTable] {
        &self.inner.tables
    }

    pub(crate) fn omnishards(&self) -> &HashSet<String> {
        &self.inner.omnisharded
    }

    /// The deployment has only one sharded table.
    pub(crate) fn common_mapping(&self) -> &Option<CommonMapping> {
        &self.inner.common_mapping
    }

    /// Find a specific sharded table.
    pub(crate) fn table(&self, name: &str) -> Option<&ShardedTable> {
        self.tables()
            .iter()
            .find(|t| t.name.as_deref() == Some(name))
    }

    /// Find out which column (if any) is sharded in the given table.
    pub(crate) fn sharded_column(&self, table: &str, columns: &[&str]) -> Option<ShardedColumn> {
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
}

#[derive(Debug, Clone)]
pub(crate) struct ShardedColumn {
    pub(crate) data_type: DataType,
    pub(crate) position: usize,
    pub(crate) centroids: Vec<Vector>,
    pub(crate) centroid_probes: usize,
}

impl ShardedColumn {
    pub(crate) fn from_sharded_table(table: &ShardedTable, columns: &[&str]) -> Option<Self> {
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
