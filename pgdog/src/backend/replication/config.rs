use super::ShardedTables;

/// Logical replication configuration.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Total number of shards.
    pub shards: usize,
    /// Sharded tables.
    pub sharded_tables: ShardedTables,
}
