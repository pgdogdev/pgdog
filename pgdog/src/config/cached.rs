use std::{collections::HashMap, ops::Deref, sync::Arc};

use indexmap::IndexMap;
use pgdog_config::{
    ConfigAndUsers, EnumeratedDatabase, OmnishardedTable, ShardedMappingDeprecated,
    ShardedMappingKey, ShardedSchema, User,
};

/// Store various calculated entities from the configuration file
/// in a hash-based format, so most of them are O(1) accessible during
/// connection pool creation.
///
/// This becomes relevant once the config grows to millions of users and databases.
///
#[derive(Debug, Default)]
pub(crate) struct CachedConfig {
    pub(super) config: Arc<ConfigAndUsers>,
    // database name -> omnisharded tables
    pub omnisharded_tables: HashMap<String, Vec<OmnishardedTable>>,
    pub sharded_mappings: IndexMap<ShardedMappingKey, Vec<ShardedMappingDeprecated>>,
    // database name -> sharded schemas
    pub sharded_schemas: HashMap<String, Vec<ShardedSchema>>,
    // name -> database
    pub databases: HashMap<String, Vec<Vec<EnumeratedDatabase>>>,
    // (user, database) -> user
    pub users: HashMap<(String, String), User>,
}

impl Deref for CachedConfig {
    type Target = Arc<ConfigAndUsers>;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl CachedConfig {
    /// Precompute shared configuration collections.
    pub(crate) fn new(config: ConfigAndUsers) -> Self {
        Self {
            omnisharded_tables: config.config.omnisharded_tables(),
            sharded_mappings: config.config.sharded_mappings(),
            sharded_schemas: config.config.sharded_schemas(),
            databases: config.config.databases(),
            users: config.users.users(),
            config: Arc::new(config),
        }
    }
}
