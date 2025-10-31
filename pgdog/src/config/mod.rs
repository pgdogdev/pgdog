//! Configuration.

// Submodules
pub mod auth;
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
pub mod url;
pub mod users;

// Re-export from error module
pub use error::Error;

// Re-export from overrides module
pub use overrides::Overrides;

// Re-export core configuration types
pub use core::{Config, ConfigAndUsers};

// Re-export from general module
pub use general::General;

// Re-export from rewrite module
pub use rewrite::{Rewrite, RewriteMode};

// Re-export from auth module
pub use auth::{AuthType, PassthoughAuth};

// Re-export from pooling module
pub use pooling::{PoolerMode, PreparedStatements, Stats};

// Re-export from database module
pub use database::{Database, LoadBalancingStrategy, ReadWriteSplit, ReadWriteStrategy, Role};

// Re-export from networking module
pub use networking::{MultiTenant, Tcp, TlsVerifyMode};

// Re-export from users module
pub use users::{Admin, Plugin, User, Users};

pub use memory::*;

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
    use crate::backend::databases::init;

    let mut config = ConfigAndUsers::default();
    config.config.databases = vec![Database {
        name: "pgdog".into(),
        host: "127.0.0.1".into(),
        port: 5432,
        ..Default::default()
    }];
    config.users.users = vec![User {
        name: "pgdog".into(),
        database: "pgdog".into(),
        password: Some("pgdog".into()),
        ..Default::default()
    }];

    set(config).unwrap();
    init();
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
    init();
}
