use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

use crate::frontend::router::sharding::Mapping;
use pgdog_config::sharding::ShardedTable as ConfigShardedTable;
pub use pgdog_config::sharding::{
    DataType, FlexibleType, Hasher, ManualQuery, OmnishardedTables, ShardedMapping,
    ShardedMappingKey, ShardedMappingKind,
};

/// Sharded table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ShardedTable {
    #[serde(flatten)]
    pub inner: ConfigShardedTable,

    /// Explicit routing rules.
    #[serde(skip, default)]
    pub mapping: Option<Mapping>,
}

impl Deref for ShardedTable {
    type Target = ConfigShardedTable;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for ShardedTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
