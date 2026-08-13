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
pub use general::{General, LogFormat};
pub use memory::*;
pub use networking::{MultiTenant, Tcp, TlsVerifyMode};
pub use overrides::Overrides;
use pgdog_config::LookupResult;
use pgdog_config::ShardedTableConfig;
pub use pgdog_config::auth::{AuthType, PassthroughAuth};
pub use pgdog_config::{LoadBalancingStrategy, ReadWriteSplit, ReadWriteStrategy};
pub use pooling::{ConnectionRecovery, PoolerMode, PreparedStatements};
pub use rewrite::{Rewrite, RewriteMode};
use std::path::Path;
pub use users::{Admin, Plugin, ServerAuth, User, Users};

// Re-export from sharding module
pub use sharding::{
    DataType, FlexibleType, Hasher, OmnishardedTables, ShardedMappingConfig,
    ShardedMappingDeprecated, ShardedMappingKindDeprecated, ShardedMappingList,
    ShardedMappingRange,
};

// Re-export from replication module
pub use replication::{MirrorConfig, Mirroring, ReplicaLag, Replication};

use parking_lot::Mutex;
use std::env;
use std::sync::Arc;

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use pgdog_stats::histogram::{self, Bounds, Normalized};
use tracing::warn;

static CONFIG: Lazy<ArcSwap<ConfigAndUsers>> =
    Lazy::new(|| ArcSwap::from_pointee(ConfigAndUsers::default()));

static LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Load configuration.
pub fn config() -> Arc<ConfigAndUsers> {
    CONFIG.load().clone()
}

/// Load the configuration file from disk.
pub fn load(config: &Path, users: &Path) -> Result<ConfigAndUsers, Error> {
    let config = ConfigAndUsers::load(config, users)?;
    set(config)
}

pub fn set(mut config: ConfigAndUsers) -> Result<ConfigAndUsers, Error> {
    config.check()?;
    validate_lookup_queries(&config)?;
    for table in config.config.sharded_tables.iter_mut() {
        // TODO: synchronous io operations inside that could be parallelized.
        // And also moved outside the configuration to the place of
        table.load_centroids()?;
    }
    set_histogram_bounds(&config);
    CONFIG.store(Arc::new(config.clone()));
    Ok(config)
}

/// Latch the process-wide query latency histogram buckets.
///
/// Bucket bounds are fixed for the life of the process: already-recorded
/// histograms are indexed by position, so re-bucketing at runtime would
/// silently reinterpret every existing sample. A reload that changes them is
/// ignored with a warning.
fn set_histogram_bounds(config: &ConfigAndUsers) {
    let (configured, normalized) =
        Bounds::from_millis_checked(&config.config.general.query_time_buckets);
    match normalized {
        Normalized::Dropped(0) => (),
        Normalized::Dropped(dropped) => {
            warn!("\"query_time_buckets\" ignored {dropped} invalid, duplicate, or excess bound(s)")
        }
        Normalized::FellBackToDefaults => {
            warn!("\"query_time_buckets\" has no usable bounds, using the defaults")
        }
    }

    if !histogram::set_bounds(configured) && *histogram::bounds() != configured {
        warn!("\"query_time_buckets\" cannot be changed at runtime, restart PgDog to apply");
    }
}

/// Validate sharding key lookup queries with the SQL parser:
/// the query must be syntactically valid, a single statement, and
/// reference exactly one parameter, `$1`. `lookup_result = "shard"`
/// requires a lookup query and never hashes, so every hash-related
/// setting on such a table is a contradiction and fails loudly.
fn validate_lookup_queries(config: &ConfigAndUsers) -> Result<(), Error> {
    for table in &config.config.sharded_tables {
        let error = |message: String| {
            Error::ParseError(format!(
                "sharded table \"{}\", column \"{}\": {}",
                table.name.as_deref().unwrap_or("*"),
                table.column,
                message,
            ))
        };

        if table.lookup_result == LookupResult::Shard {
            if table.lookup_query.is_none() {
                return Err(error(
                    "\"lookup_result = 'shard'\" requires a \"lookup_query\"".into(),
                ));
            }
            if table.mapping.is_some() {
                return Err(error(
                    "\"lookup_result = 'shard'\" never hashes and can't be combined with \"mapping\""
                        .into(),
                ));
            }
            if !table.centroids.is_empty() || table.centroids_path.is_some() {
                return Err(error(
                    "\"lookup_result = 'shard'\" never hashes and can't be combined with centroids"
                        .into(),
                ));
            }
            if table.hasher != Hasher::default() {
                return Err(error(
                    "\"lookup_result = 'shard'\" never hashes and can't be combined with \"hasher\""
                        .into(),
                ));
            }
        }

        let Some(query) = &table.lookup_query else {
            continue;
        };

        validate_lookup_query(query).map_err(error)?;
    }

    Ok(())
}

#[cfg(feature = "new_parser")]
fn validate_lookup_query(query: &str) -> Result<(), String> {
    use itertools::Itertools;
    use pg_raw_parse::{
        Node,
        walk::{Recurse, walk_manual},
    };
    use std::ops::ControlFlow;

    let ast = pg_raw_parse::parse(query)
        .map_err(|err| format!("\"lookup_query\" is invalid: {}", err))?;
    let stmt = ast
        .stmts()
        .exactly_one()
        .map_err(|_| "\"lookup_query\" must be a single statement".to_string())?;

    let mut saw_param = false;
    let other_param = walk_manual(stmt, |node| match node {
        Node::ParamRef(param) if param.number != 1 => ControlFlow::Break(()),
        Node::ParamRef(_) => {
            saw_param = true;
            Recurse::yes()
        }
        _ => Recurse::yes(),
    })
    .is_some();

    if other_param || !saw_param {
        return Err("\"lookup_query\" must reference exactly one parameter, \"$1\"".into());
    }

    Ok(())
}

#[cfg(not(feature = "new_parser"))]
fn validate_lookup_query(query: &str) -> Result<(), String> {
    use std::collections::HashSet;

    let ast =
        pg_query::parse(query).map_err(|err| format!("\"lookup_query\" is invalid: {}", err))?;

    if ast.protobuf.stmts.len() != 1 {
        return Err("\"lookup_query\" must be a single statement".into());
    }

    // Best effort: `nodes()` covers the node types a lookup query
    // realistically uses, but isn't guaranteed to visit every subtree.
    // A parameter it misses fails loudly at runtime instead, when the
    // lookup query runs with a single bound value.
    let params = ast
        .protobuf
        .nodes()
        .into_iter()
        .filter_map(|(node, ..)| match node {
            pg_query::NodeRef::ParamRef(param) => Some(param.number),
            _ => None,
        })
        .collect::<HashSet<_>>();

    if params != HashSet::from([1]) {
        return Err("\"lookup_query\" must reference exactly one parameter, \"$1\"".into());
    }

    Ok(())
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
    load_test_with_user_and_pooler_mode("pgdog", pooler_mode, Role::default())
}

#[cfg(test)]
pub fn load_test_with_user(user: &str) {
    load_test_with_user_and_pooler_mode(user, PoolerMode::Transaction, Role::Primary)
}

#[cfg(test)]
fn load_test_with_user_and_pooler_mode(user: &str, pooler_mode: PoolerMode, role: Role) {
    use crate::backend::databases::init;

    let mut config = ConfigAndUsers::default();
    config.config.databases = vec![Database {
        name: "pgdog".into(),
        host: "127.0.0.1".into(),
        port: 5432,
        pooler_mode: Some(pooler_mode),
        role,
        ..Default::default()
    }];
    config.users.users = vec![User {
        name: user.into(),
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
    load_test_sharded_n(2);
}

/// Load 3-shard test configuration.
pub fn load_test_sharded_3() {
    load_test_sharded_n(3);
}

fn load_test_sharded_n(num_shards: usize) {
    use pgdog_config::{OmnishardedTables, ShardedSchema};

    use crate::backend::databases::init;

    let mut config = ConfigAndUsers::default();
    config.config.general.min_pool_size = 0;
    config.config.databases = (0..num_shards)
        .flat_map(|shard| {
            vec![
                Database {
                    name: "pgdog".into(),
                    host: "127.0.0.1".into(),
                    port: 5432,
                    role: Role::Primary,
                    database_name: Some(format!("shard_{}", shard)),
                    shard,
                    ..Default::default()
                },
                Database {
                    name: "pgdog".into(),
                    host: "127.0.0.1".into(),
                    port: 5432,
                    role: Role::Replica,
                    read_only: Some(true),
                    database_name: Some(format!("shard_{}", shard)),
                    shard,
                    ..Default::default()
                },
            ]
        })
        .collect();
    config.config.sharded_tables = vec![
        ShardedTableConfig {
            database: "pgdog".into(),
            name: Some("sharded".into()),
            column: "id".into(),
            ..Default::default()
        },
        ShardedTableConfig {
            database: "pgdog".into(),
            name: Some("sharded_varchar".into()),
            column: "id_varchar".into(),
            data_type: DataType::Varchar,
            ..Default::default()
        },
        ShardedTableConfig {
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

#[cfg(test)]
mod lookup_query_tests {
    use super::*;

    fn config_with_query(query: &str) -> ConfigAndUsers {
        let source = format!(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_query = "{}"
"#,
            query
        );

        ConfigAndUsers {
            config: toml::from_str(&source).unwrap(),
            ..Default::default()
        }
    }

    #[test]
    fn test_lookup_query_valid() {
        let config =
            config_with_query("SELECT COALESCE(parent_tenant_id, id) FROM tenants WHERE id = $1");
        validate_lookup_queries(&config).unwrap();
    }

    fn config_from(source: &str) -> ConfigAndUsers {
        ConfigAndUsers {
            config: toml::from_str(source).unwrap(),
            ..Default::default()
        }
    }

    #[test]
    fn test_lookup_result_shard_requires_query() {
        let config = config_from(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_result = "shard"
"#,
        );
        assert!(validate_lookup_queries(&config).is_err());
    }

    #[test]
    fn test_lookup_result_shard_never_hashes_config() {
        // Valid: query + shard mode, no hash-related settings.
        let config = config_from(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_query = "SELECT shard_id FROM tenants WHERE id = $1"
lookup_result = "shard"
"#,
        );
        validate_lookup_queries(&config).unwrap();

        // Mapping contradicts shard mode.
        let config = config_from(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_query = "SELECT shard_id FROM tenants WHERE id = $1"
lookup_result = "shard"

[[sharded_tables.mapping]]
values = [1]
shard = 0
"#,
        );
        assert!(validate_lookup_queries(&config).is_err());

        // Centroids contradict shard mode.
        let config = config_from(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_query = "SELECT shard_id FROM tenants WHERE id = $1"
lookup_result = "shard"
centroids = [[1.0, 2.0]]
"#,
        );
        assert!(validate_lookup_queries(&config).is_err());

        // A non-default hasher contradicts shard mode.
        let config = config_from(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_query = "SELECT shard_id FROM tenants WHERE id = $1"
lookup_result = "shard"
hasher = "sha1"
"#,
        );
        assert!(validate_lookup_queries(&config).is_err());
    }

    #[test]
    fn test_lookup_query_requires_valid_sql() {
        let config = config_with_query("SELEC oops");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("invalid"));
    }

    #[test]
    fn test_lookup_query_requires_single_statement() {
        let config = config_with_query("SELECT 1; SELECT 2");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("single statement"));
    }

    #[test]
    fn test_lookup_query_requires_placeholder() {
        let config = config_with_query("SELECT parent FROM tenants");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("$1"));

        let config = config_with_query("SELECT parent FROM tenants WHERE id = $1 AND x = $2");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("$1"));

        let config = config_with_query("SELECT parent FROM tenants WHERE x = $2");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("$1"));
    }
}
