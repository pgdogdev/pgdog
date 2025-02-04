use super::ShardedTables;

#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub shards: usize,
    pub sharded_tables: ShardedTables,
}

impl ReplicationConfig {
    pub fn sharded_column(&self, table: &str, columns: &[&str]) -> Option<usize> {
        self.sharded_tables.sharded_column(table, columns)
    }

    pub fn shards(&self) -> usize {
        self.shards
    }
}
