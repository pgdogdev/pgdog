use super::{ShardedColumn, ShardedTables};

/// Logical replication configuration.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Total number of shards.
    pub shards: usize,
    /// Sharded tables.
    pub sharded_tables: ShardedTables,
}

impl ReplicationConfig {
    /// Get the position of the sharded column in a row.
    pub fn sharded_column(&self, table: &str, columns: &[&str]) -> Option<ShardedColumn> {
        self.sharded_tables.sharded_column(table, columns)
    }

    /// Total number of shards.
    pub fn shards(&self) -> usize {
        self.shards
    }
}
