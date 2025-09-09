use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::read_to_string;
use std::path::PathBuf;
use tracing::{info, warn};

use super::database::Database;
use super::error::Error;
use super::general::General;
use super::networking::{MultiTenant, Tcp};
use super::pooling::{PoolerMode, Stats};
use super::replication::{MirrorConfig, Mirroring, ReplicaLag, Replication};
use super::sharding::{ManualQuery, OmnishardedTables, ShardedMapping, ShardedTable};
use super::users::{Admin, Plugin, Users};

#[derive(Debug, Clone)]
pub struct ConfigAndUsers {
    /// pgdog.toml
    pub config: Config,
    /// users.toml
    pub users: Users,
    /// Path to pgdog.toml.
    pub config_path: PathBuf,
    /// Path to users.toml.
    pub users_path: PathBuf,
}

impl ConfigAndUsers {
    /// Load configuration from disk or use defaults.
    pub fn load(config_path: &PathBuf, users_path: &PathBuf) -> Result<Self, Error> {
        let config: Config = if let Ok(config) = read_to_string(config_path) {
            let config = match toml::from_str(&config) {
                Ok(config) => config,
                Err(err) => return Err(Error::config(&config, err)),
            };
            info!("loaded \"{}\"", config_path.display());
            config
        } else {
            warn!(
                "\"{}\" doesn't exist, loading defaults instead",
                config_path.display()
            );
            Config::default()
        };

        if config.admin.random() {
            #[cfg(debug_assertions)]
            info!("[debug only] admin password: {}", config.admin.password);
            #[cfg(not(debug_assertions))]
            warn!("admin password has been randomly generated");
        }

        if config.multi_tenant.is_some() {
            info!("multi-tenant protection enabled");
        }

        let users: Users = if let Ok(users) = read_to_string(users_path) {
            let mut users: Users = toml::from_str(&users)?;
            users.check(&config);
            info!("loaded \"{}\"", users_path.display());
            users
        } else {
            warn!(
                "\"{}\" doesn't exist, loading defaults instead",
                users_path.display()
            );
            Users::default()
        };

        Ok(ConfigAndUsers {
            config,
            users,
            config_path: config_path.to_owned(),
            users_path: users_path.to_owned(),
        })
    }

    /// Prepared statements are enabled.
    pub fn prepared_statements(&self) -> bool {
        // Disable prepared statements automatically in session mode
        if self.config.general.pooler_mode == PoolerMode::Session {
            false
        } else {
            self.config.general.prepared_statements.enabled()
        }
    }

    /// Prepared statements are in "full" mode (used for query parser decision).
    pub fn prepared_statements_full(&self) -> bool {
        self.config.general.prepared_statements.full()
    }

    pub fn pub_sub_enabled(&self) -> bool {
        self.config.general.pub_sub_channel_size > 0
    }
}

impl Default for ConfigAndUsers {
    fn default() -> Self {
        Self {
            config: Config::default(),
            users: Users::default(),
            config_path: PathBuf::from("pgdog.toml"),
            users_path: PathBuf::from("users.toml"),
        }
    }
}

/// Configuration.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// General configuration.
    #[serde(default)]
    pub general: General,

    /// Statistics.
    #[serde(default)]
    pub stats: Stats,

    /// TCP settings
    #[serde(default)]
    pub tcp: Tcp,

    /// Multi-tenant
    pub multi_tenant: Option<MultiTenant>,

    /// Servers.
    #[serde(default)]
    pub databases: Vec<Database>,

    #[serde(default)]
    pub plugins: Vec<Plugin>,

    #[serde(default)]
    pub admin: Admin,

    /// List of sharded tables.
    #[serde(default)]
    pub sharded_tables: Vec<ShardedTable>,

    /// Queries routed manually to a single shard.
    #[serde(default)]
    pub manual_queries: Vec<ManualQuery>,

    /// List of omnisharded tables.
    #[serde(default)]
    pub omnisharded_tables: Vec<OmnishardedTables>,

    /// Explicit sharding key mappings.
    #[serde(default)]
    pub sharded_mappings: Vec<ShardedMapping>,

    /// Replica lag configuration.
    #[serde(default, deserialize_with = "ReplicaLag::deserialize_optional")]
    pub replica_lag: Option<ReplicaLag>,

    /// Replication config.
    #[serde(default)]
    pub replication: Replication,

    /// Mirroring configurations.
    #[serde(default)]
    pub mirroring: Vec<Mirroring>,
}

impl Config {
    /// Organize all databases by name for quicker retrieval.
    pub fn databases(&self) -> HashMap<String, Vec<Vec<Database>>> {
        let mut databases = HashMap::new();
        for database in &self.databases {
            let entry = databases
                .entry(database.name.clone())
                .or_insert_with(Vec::new);
            while entry.len() <= database.shard {
                entry.push(vec![]);
            }
            entry
                .get_mut(database.shard)
                .unwrap()
                .push(database.clone());
        }
        databases
    }

    /// Organize sharded tables by database name.
    pub fn sharded_tables(&self) -> HashMap<String, Vec<ShardedTable>> {
        let mut tables = HashMap::new();

        for table in &self.sharded_tables {
            let entry = tables
                .entry(table.database.clone())
                .or_insert_with(Vec::new);
            entry.push(table.clone());
        }

        tables
    }

    pub fn omnisharded_tables(&self) -> HashMap<String, Vec<String>> {
        let mut tables = HashMap::new();

        for table in &self.omnisharded_tables {
            let entry = tables
                .entry(table.database.clone())
                .or_insert_with(Vec::new);
            for t in &table.tables {
                entry.push(t.clone());
            }
        }

        tables
    }

    /// Manual queries.
    pub fn manual_queries(&self) -> HashMap<String, ManualQuery> {
        let mut queries = HashMap::new();

        for query in &self.manual_queries {
            queries.insert(query.fingerprint.clone(), query.clone());
        }

        queries
    }

    /// Sharded mappings.
    pub fn sharded_mappings(
        &self,
    ) -> HashMap<(String, String, Option<String>), Vec<ShardedMapping>> {
        let mut mappings = HashMap::new();

        for mapping in &self.sharded_mappings {
            let mapping = mapping.clone();
            let entry = mappings
                .entry((
                    mapping.database.clone(),
                    mapping.column.clone(),
                    mapping.table.clone(),
                ))
                .or_insert_with(Vec::new);
            entry.push(mapping);
        }

        mappings
    }

    pub fn check(&self) {
        // Check databases.
        let mut duplicate_primaries = HashSet::new();
        for database in self.databases.clone() {
            let id = (
                database.name.clone(),
                database.role,
                database.shard,
                database.port,
            );
            let new = duplicate_primaries.insert(id);
            if !new {
                warn!(
                    "database \"{}\" (shard={}) has a duplicate {}",
                    database.name, database.shard, database.role,
                );
            }
        }
    }

    /// Multi-tenanncy is enabled.
    pub fn multi_tenant(&self) -> &Option<MultiTenant> {
        &self.multi_tenant
    }

    /// Get mirroring configuration for a specific source/destination pair.
    pub fn get_mirroring_config(
        &self,
        source_db: &str,
        destination_db: &str,
    ) -> Option<MirrorConfig> {
        self.mirroring
            .iter()
            .find(|m| m.source_db == source_db && m.destination_db == destination_db)
            .map(|m| MirrorConfig {
                queue_length: m.queue_length.unwrap_or(self.general.mirror_queue),
                exposure: m.exposure.unwrap_or(self.general.mirror_exposure),
            })
    }

    /// Get all mirroring configurations mapped by source database.
    pub fn mirroring_by_source(&self) -> HashMap<String, Vec<(String, MirrorConfig)>> {
        let mut result = HashMap::new();

        for mirror in &self.mirroring {
            let config = MirrorConfig {
                queue_length: mirror.queue_length.unwrap_or(self.general.mirror_queue),
                exposure: mirror.exposure.unwrap_or(self.general.mirror_exposure),
            };

            result
                .entry(mirror.source_db.clone())
                .or_insert_with(Vec::new)
                .push((mirror.destination_db.clone(), config));
        }

        result
    }
}
