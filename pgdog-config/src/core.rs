use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::read_to_string;
use std::path::PathBuf;
use tracing::{info, warn};

use crate::sharding::ShardedSchema;
use crate::{Memory, PassthoughAuth, PreparedStatements};

use super::database::Database;
use super::error::Error;
use super::general::General;
use super::networking::{MultiTenant, Tcp};
use super::pooling::{PoolerMode, Stats};
use super::replication::{MirrorConfig, Mirroring, ReplicaLag, Replication};
use super::rewrite::Rewrite;
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
        let mut config: Config = if let Ok(config) = read_to_string(config_path) {
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

        if config.multi_tenant.is_some() {
            info!("multi-tenant protection enabled");
        }

        let mut users: Users = if let Ok(users) = read_to_string(users_path) {
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

        // Override admin set in pgdog.toml
        // with what's in users.toml.
        if let Some(admin) = users.admin.take() {
            config.admin = admin;
        }

        if config.admin.random() {
            #[cfg(debug_assertions)]
            info!("[debug only] admin password: {}", config.admin.password);
            #[cfg(not(debug_assertions))]
            warn!("admin password has been randomly generated");
        }

        Ok(ConfigAndUsers {
            config,
            users,
            config_path: config_path.to_owned(),
            users_path: users_path.to_owned(),
        })
    }

    /// Prepared statements are enabled.
    pub fn prepared_statements(&self) -> PreparedStatements {
        // Disable prepared statements automatically in session mode
        if self.config.general.pooler_mode == PoolerMode::Session {
            PreparedStatements::Disabled
        } else {
            self.config.general.prepared_statements
        }
    }

    /// Prepared statements are in "full" mode (used for query parser decision).
    pub fn prepared_statements_full(&self) -> bool {
        self.config.general.prepared_statements.full()
    }

    pub fn query_parser_enabled(&self) -> bool {
        self.config.general.query_parser_enabled
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

    /// Rewrite configuration.
    #[serde(default)]
    pub rewrite: Rewrite,

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

    #[serde(default)]
    pub sharded_schemas: Vec<ShardedSchema>,

    /// Replica lag configuration.
    #[serde(default, deserialize_with = "ReplicaLag::deserialize_optional")]
    pub replica_lag: Option<ReplicaLag>,

    /// Replication config.
    #[serde(default)]
    pub replication: Replication,

    /// Mirroring configurations.
    #[serde(default)]
    pub mirroring: Vec<Mirroring>,

    /// Memory tweaks
    #[serde(default)]
    pub memory: Memory,
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

    pub fn sharded_schemas(&self) -> HashMap<String, Vec<ShardedSchema>> {
        let mut schemas = HashMap::new();

        for schema in &self.sharded_schemas {
            let entry = schemas
                .entry(schema.database.clone())
                .or_insert_with(Vec::new);
            entry.push(schema.clone());
        }

        schemas
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
        let mut duplicate_dbs = HashSet::new();
        for database in self.databases.clone() {
            let id = (
                database.name.clone(),
                database.role,
                database.shard,
                database.port,
                database.host.clone(),
            );
            let new = duplicate_dbs.insert(id);
            if !new {
                warn!(
                    "database \"{}\" (shard={}) has a duplicate {}",
                    database.name, database.shard, database.role,
                );
            }
        }

        // Check that idle_healthcheck_interval is shorter than ban_timeout.
        if self.general.ban_timeout > 0
            && self.general.idle_healthcheck_interval >= self.general.ban_timeout
        {
            warn!(
                "idle_healthcheck_interval ({}ms) should be shorter than ban_timeout ({}ms) to ensure health checks are triggered before a ban expires",
                self.general.idle_healthcheck_interval, self.general.ban_timeout
            );
        }

        // Warn about plain auth and TLS
        match self.general.passthrough_auth {
            PassthoughAuth::Enabled if !self.general.tls_client_required => {
                warn!(
                    "consider setting tls_client_required while passthrough_auth is enabled to prevent clients from exposing plaintext passwords"
                );
            }
            PassthoughAuth::EnabledPlain => {
                warn!(
                    "passthrough_auth plain is enabled - network traffic may expose plaintext passwords"
                )
            }
            _ => (),
        }
    }

    /// Multi-tenancy is enabled.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PoolerMode, PreparedStatements};
    use std::time::Duration;

    #[test]
    fn test_basic() {
        let source = r#"
[general]
host = "0.0.0.0"
port = 6432
default_pool_size = 15
pooler_mode = "transaction"

[[databases]]
name = "production"
role = "primary"
host = "127.0.0.1"
port = 5432
database_name = "postgres"

[tcp]
keepalive = true
interval = 5000
time = 1000
user_timeout = 1000
retries = 5

[[plugins]]
name = "pgdog_routing"

[multi_tenant]
column = "tenant_id"
"#;

        let config: Config = toml::from_str(source).unwrap();
        assert_eq!(config.databases[0].name, "production");
        assert_eq!(config.plugins[0].name, "pgdog_routing");
        assert!(config.tcp.keepalive());
        assert_eq!(config.tcp.interval().unwrap(), Duration::from_millis(5000));
        assert_eq!(
            config.tcp.user_timeout().unwrap(),
            Duration::from_millis(1000)
        );
        assert_eq!(config.tcp.time().unwrap(), Duration::from_millis(1000));
        assert_eq!(config.tcp.retries().unwrap(), 5);
        assert_eq!(config.multi_tenant.unwrap().column, "tenant_id");
    }

    #[test]
    fn test_prepared_statements_disabled_in_session_mode() {
        let mut config = ConfigAndUsers::default();

        // Test transaction mode (default) - prepared statements should be enabled
        config.config.general.pooler_mode = PoolerMode::Transaction;
        config.config.general.prepared_statements = PreparedStatements::Extended;
        assert_eq!(
            config.prepared_statements(),
            PreparedStatements::Extended,
            "Prepared statements should be enabled in transaction mode"
        );

        // Test session mode - prepared statements should be disabled
        config.config.general.pooler_mode = PoolerMode::Session;
        config.config.general.prepared_statements = PreparedStatements::Extended;
        assert_eq!(
            config.prepared_statements(),
            PreparedStatements::Disabled,
            "Prepared statements should be disabled in session mode"
        );

        // Test session mode with full prepared statements - should still be disabled
        config.config.general.pooler_mode = PoolerMode::Session;
        config.config.general.prepared_statements = PreparedStatements::Full;
        assert_eq!(
            config.prepared_statements(),
            PreparedStatements::Disabled,
            "Prepared statements should be disabled in session mode even when set to Full"
        );

        // Test transaction mode with disabled prepared statements - should remain disabled
        config.config.general.pooler_mode = PoolerMode::Transaction;
        config.config.general.prepared_statements = PreparedStatements::Disabled;
        assert_eq!(
            config.prepared_statements(),
            PreparedStatements::Disabled,
            "Prepared statements should remain disabled when explicitly set to Disabled in transaction mode"
        );
    }

    #[test]
    fn test_mirroring_config() {
        let source = r#"
[general]
host = "0.0.0.0"
port = 6432
mirror_queue = 128
mirror_exposure = 1.0

[[databases]]
name = "source_db"
host = "127.0.0.1"
port = 5432

[[databases]]
name = "destination_db1"
host = "127.0.0.1"
port = 5433

[[databases]]
name = "destination_db2"
host = "127.0.0.1"
port = 5434

[[mirroring]]
source_db = "source_db"
destination_db = "destination_db1"
queue_length = 256
exposure = 0.5

[[mirroring]]
source_db = "source_db"
destination_db = "destination_db2"
exposure = 0.75
"#;

        let config: Config = toml::from_str(source).unwrap();

        // Verify we have 2 mirroring configurations
        assert_eq!(config.mirroring.len(), 2);

        // Check first mirroring config
        assert_eq!(config.mirroring[0].source_db, "source_db");
        assert_eq!(config.mirroring[0].destination_db, "destination_db1");
        assert_eq!(config.mirroring[0].queue_length, Some(256));
        assert_eq!(config.mirroring[0].exposure, Some(0.5));

        // Check second mirroring config
        assert_eq!(config.mirroring[1].source_db, "source_db");
        assert_eq!(config.mirroring[1].destination_db, "destination_db2");
        assert_eq!(config.mirroring[1].queue_length, None); // Should use global default
        assert_eq!(config.mirroring[1].exposure, Some(0.75));

        // Verify global defaults are still set
        assert_eq!(config.general.mirror_queue, 128);
        assert_eq!(config.general.mirror_exposure, 1.0);

        // Test get_mirroring_config method
        let mirror_config = config
            .get_mirroring_config("source_db", "destination_db1")
            .unwrap();
        assert_eq!(mirror_config.queue_length, 256);
        assert_eq!(mirror_config.exposure, 0.5);

        let mirror_config2 = config
            .get_mirroring_config("source_db", "destination_db2")
            .unwrap();
        assert_eq!(mirror_config2.queue_length, 128); // Uses global default
        assert_eq!(mirror_config2.exposure, 0.75);

        // Non-existent mirror config should return None
        assert!(config
            .get_mirroring_config("source_db", "non_existent")
            .is_none());
    }

    #[test]
    fn test_admin_override_from_users_toml() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let pgdog_config = r#"
[admin]
name = "pgdog_admin"
user = "pgdog_admin_user"
password = "pgdog_admin_password"
"#;

        let users_config = r#"
[admin]
name = "users_admin"
user = "users_admin_user"
password = "users_admin_password"
"#;

        let mut pgdog_file = NamedTempFile::new().unwrap();
        let mut users_file = NamedTempFile::new().unwrap();

        pgdog_file.write_all(pgdog_config.as_bytes()).unwrap();
        users_file.write_all(users_config.as_bytes()).unwrap();

        pgdog_file.flush().unwrap();
        users_file.flush().unwrap();

        let config_and_users =
            ConfigAndUsers::load(&pgdog_file.path().into(), &users_file.path().into()).unwrap();

        assert_eq!(config_and_users.config.admin.name, "users_admin");
        assert_eq!(config_and_users.config.admin.user, "users_admin_user");
        assert_eq!(
            config_and_users.config.admin.password,
            "users_admin_password"
        );
        assert!(config_and_users.users.admin.is_none());
    }

    #[test]
    fn test_admin_override_with_default_config() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let pgdog_config = r#"
[general]
host = "0.0.0.0"
port = 6432
"#;

        let users_config = r#"
[admin]
name = "users_admin"
user = "users_admin_user"
password = "users_admin_password"
"#;

        let mut pgdog_file = NamedTempFile::new().unwrap();
        let mut users_file = NamedTempFile::new().unwrap();

        pgdog_file.write_all(pgdog_config.as_bytes()).unwrap();
        users_file.write_all(users_config.as_bytes()).unwrap();

        pgdog_file.flush().unwrap();
        users_file.flush().unwrap();

        let config_and_users =
            ConfigAndUsers::load(&pgdog_file.path().into(), &users_file.path().into()).unwrap();

        assert_eq!(config_and_users.config.admin.name, "users_admin");
        assert_eq!(config_and_users.config.admin.user, "users_admin_user");
        assert_eq!(
            config_and_users.config.admin.password,
            "users_admin_password"
        );
        assert!(config_and_users.users.admin.is_none());
    }
}
