use super::{ShardedColumn, ShardedTables};

/// Logical replication configuration.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationConfig {
    /// Total number of shards.
    pub(crate) shards: usize,
    /// Sharded tables.
    pub(crate) sharded_tables: ShardedTables,
}

impl ReplicationConfig {
    /// Get the position of the sharded column in a row.
    pub(crate) fn sharded_column(&self, table: &str, columns: &[&str]) -> Option<ShardedColumn> {
        self.sharded_tables.sharded_column(table, columns)
    }

    /// Total number of shards.
    pub(crate) fn shards(&self) -> usize {
        self.shards
    }
}
