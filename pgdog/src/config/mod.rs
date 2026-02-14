//! Configuration.

// Submodules
pub mod convert;
pub mod core;
pub mod database;
pub mod error;
pub mod general;
pub mod memory;
pub mod networking;
pub mod overrides;
pub mod pooling;
pub mod replication;
pub mod rewrite;
pub mod sharding;
pub mod users;

pub use core::{Config, ConfigAndUsers};
pub use database::{Database, Role};
pub use error::Error;
pub use general::General;
pub use memory::*;
pub use networking::{MultiTenant, Tcp, TlsVerifyMode};
pub use overrides::Overrides;
pub use pgdog_config::auth::{AuthType, PassthoughAuth};
pub use pgdog_config::{LoadBalancingStrategy, ReadWriteSplit, ReadWriteStrategy};
pub use pooling::{ConnectionRecovery, PoolerMode, PreparedStatements};
pub use rewrite::{Rewrite, RewriteMode};
pub use users::{Admin, Plugin, User, Users};

// Re-export from sharding module
pub use sharding::{
    DataType, FlexibleType, Hasher, ManualQuery, OmnishardedTables, ShardedMapping,
    ShardedMappingKind, ShardedTable,
};

// Re-export from replication module
pub use replication::{MirrorConfig, Mirroring, ReplicaLag, Replication};

use parking_lot::Mutex;
use std::sync::Arc;
use std::{env, path::PathBuf};

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;

static CONFIG: Lazy<ArcSwap<ConfigAndUsers>> =
    Lazy::new(|| ArcSwap::from_pointee(ConfigAndUsers::default()));

static LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Load configuration.
pub fn config() -> Arc<ConfigAndUsers> {
    CONFIG.load().clone()
}

/// Load the configuration file from disk.
pub fn load(config: &PathBuf, users: &PathBuf) -> Result<ConfigAndUsers, Error> {
    let config = ConfigAndUsers::load(config, users)?;
    set(config)
}

pub fn set(mut config: ConfigAndUsers) -> Result<ConfigAndUsers, Error> {
    config.config.check();
    for table in config.config.sharded_tables.iter_mut() {
        table.load_centroids()?;
    }
    CONFIG.store(Arc::new(config.clone()));
    Ok(config)
}

/// Load configuration from a list of database URLs.
pub fn from_urls(urls: &[String]) -> Result<ConfigAndUsers, Error> {
    let _lock = LOCK.lock();
    let config = (*config()).clone();
    let config = config.databases_from_urls(urls)?;
    CONFIG.store(Arc::new(config.clone()));
    Ok(config)
}

/// Extract all database URLs from the environment and
/// create the config.
pub fn from_env() -> Result<ConfigAndUsers, Error> {
    let _lock = LOCK.lock();

    let mut urls = vec![];
    let mut index = 1;
    while let Ok(url) = env::var(format!("PGDOG_DATABASE_URL_{}", index)) {
        urls.push(url);
        index += 1;
    }

    if urls.is_empty() {
        return Err(Error::NoDbsInEnv);
    }

    let mut config = (*config()).clone();
    config = config.databases_from_urls(&urls)?;

    // Extract mirroring configuration
    let mut mirror_strs = vec![];
    let mut index = 1;
    while let Ok(mirror_str) = env::var(format!("PGDOG_MIRRORING_{}", index)) {
        mirror_strs.push(mirror_str);
        index += 1;
    }

    if !mirror_strs.is_empty() {
        config = config.mirroring_from_strings(&mirror_strs)?;
    }

    CONFIG.store(Arc::new(config.clone()));
    Ok(config)
}

/// Override some settings.
pub fn overrides(overrides: Overrides) {
    let mut config = (*config()).clone();
    let Overrides {
        default_pool_size,
        min_pool_size,
        session_mode,
    } = overrides;

    if let Some(default_pool_size) = default_pool_size {
        config.config.general.default_pool_size = default_pool_size;
    }

    if let Some(min_pool_size) = min_pool_size {
        config.config.general.min_pool_size = min_pool_size;
    }

    if let Some(session_mode) = session_mode {
        config.config.general.pooler_mode = if session_mode {
            PoolerMode::Session
        } else {
            PoolerMode::Transaction
        };
    }

    CONFIG.store(Arc::new(config));
}

// Test helper functions
#[cfg(test)]
pub fn load_test() {
    load_test_with_pooler_mode(PoolerMode::Transaction)
}

#[cfg(test)]
pub fn load_test_with_pooler_mode(pooler_mode: PoolerMode) {
    use crate::backend::databases::init;

    let mut config = ConfigAndUsers::default();
    config.config.databases = vec![Database {
        name: "pgdog".into(),
        host: "127.0.0.1".into(),
        port: 5432,
        pooler_mode: Some(pooler_mode),
        ..Default::default()
    }];
    config.users.users = vec![User {
        name: "pgdog".into(),
        database: "pgdog".into(),
        password: Some("pgdog".into()),
        pooler_mode: Some(pooler_mode),
        ..Default::default()
    }];

    set(config).unwrap();
    init().unwrap();
}

#[cfg(test)]
pub fn load_test_replicas() {
    use crate::backend::databases::init;

    let mut config = ConfigAndUsers::default();
    config.config.databases = vec![
        Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 5432,
            role: Role::Primary,
            ..Default::default()
        },
        Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 5432,
            role: Role::Replica,
            read_only: Some(true),
            ..Default::default()
        },
    ];
    config.config.general.load_balancing_strategy = LoadBalancingStrategy::RoundRobin;
    config.users.users = vec![User {
        name: "pgdog".into(),
        database: "pgdog".into(),
        password: Some("pgdog".into()),
        ..Default::default()
    }];

    set(config).unwrap();
    init().unwrap();
}

#[cfg(test)]
pub fn load_test_sharded() {
    use pgdog_config::{OmnishardedTables, ShardedSchema};

    use crate::backend::databases::init;

    let mut config = ConfigAndUsers::default();
    config.config.general.min_pool_size = 0;
    config.config.databases = vec![
        Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 5432,
            role: Role::Primary,
            database_name: Some("shard_0".into()),
            shard: 0,
            ..Default::default()
        },
        Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 5432,
            role: Role::Replica,
            read_only: Some(true),
            database_name: Some("shard_0".into()),
            shard: 0,
            ..Default::default()
        },
        Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 5432,
            role: Role::Primary,
            database_name: Some("shard_1".into()),
            shard: 1,
            ..Default::default()
        },
        Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 5432,
            role: Role::Replica,
            read_only: Some(true),
            database_name: Some("shard_1".into()),
            shard: 1,
            ..Default::default()
        },
    ];
    config.config.sharded_tables = vec![
        ShardedTable {
            database: "pgdog".into(),
            name: Some("sharded".into()),
            column: "id".into(),
            ..Default::default()
        },
        ShardedTable {
            database: "pgdog".into(),
            name: Some("sharded_varchar".into()),
            column: "id_varchar".into(),
            data_type: DataType::Varchar,
            ..Default::default()
        },
        ShardedTable {
            database: "pgdog".into(),
            name: Some("sharded_uuid".into()),
            column: "id_uuid".into(),
            data_type: DataType::Uuid,
            ..Default::default()
        },
    ];
    config.config.sharded_schemas = vec![
        ShardedSchema {
            database: "pgdog".into(),
            name: Some("acustomer".into()),
            shard: 0,
            ..Default::default()
        },
        ShardedSchema {
            database: "pgdog".into(),
            name: Some("bcustomer".into()),
            shard: 1,
            ..Default::default()
        },
        ShardedSchema {
            database: "pgdog".into(),
            name: Some("all".into()),
            all: true,
            ..Default::default()
        },
    ];
    config.config.omnisharded_tables = vec![OmnishardedTables {
        database: "pgdog".into(),
        tables: vec!["sharded_omni".into()],
        sticky: false,
    }];
    config.config.rewrite.enabled = true;
    config.config.rewrite.split_inserts = RewriteMode::Rewrite;
    config.config.rewrite.shard_key = RewriteMode::Rewrite;
    config.config.general.load_balancing_strategy = LoadBalancingStrategy::RoundRobin;
    config.users.users = vec![User {
        name: "pgdog".into(),
        database: "pgdog".into(),
        password: Some("pgdog".into()),
        ..Default::default()
    }];

    set(config).unwrap();
    init().unwrap();
}
