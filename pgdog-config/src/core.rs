use indexmap::IndexMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::read_to_string;
use std::path::{Path, PathBuf};
use tracing::{error, info, warn};

use crate::sharding::ShardedSchema;
use crate::util::random_string;
use crate::{
    EnumeratedDatabase, Memory, OmnishardedTable, PassthroughAuth, PreparedStatements, QueryParser,
    QueryParserEngine, QueryParserLevel, ReadWriteSplit, RewriteMode, Role, ShardedMappingKey,
    ShardedTableConfig, SystemCatalogsBehavior, system_catalogs,
};

use super::database::Database;
use super::error::Error;
use super::general::General;
use super::networking::{MultiTenant, Tcp, TlsVerifyMode};
use super::otel::Otel;
use super::pooling::PoolerMode;
use super::replication::{MirrorConfig, Mirroring, MirroringLevel, ReplicaLag, Replication};
use super::rewrite::Rewrite;
use super::sharding::{ManualQuery, OmnishardedTables, ShardedMappingDeprecated};
use super::users::{Admin, Plugin, Users};
use super::vault::Vault;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConfigAndUsers {
    /// parsed pgdog.toml or default [Config]
    pub config: Config,
    /// parsed users.toml or default [Users]
    pub users: Users,
    /// Path to pgdog.toml.
    pub config_path: PathBuf,
    /// Path to users.toml.
    pub users_path: PathBuf,
    /// Raw, unparsed text of `pgdog.toml`.
    /// None when the file is missing.
    pub config_text: Option<String>,
    /// Raw, unparsed text of `users.toml`.
    /// None when the file is missing.
    pub users_text: Option<String>,
}

impl ConfigAndUsers {
    /// Load configuration from disk or use defaults.
    pub fn load(config_path: &Path, users_path: &Path) -> Result<Self, Error> {
        let config_text = read_to_string(config_path).ok();
        let mut config: Config = if let Some(text) = &config_text {
            let config = match toml::from_str(text) {
                Ok(config) => config,
                Err(err) => {
                    let error = Error::config(text, err);
                    error!("failed to load {}: {}", config_path.display(), error);
                    return Err(error);
                }
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

        let users_text = read_to_string(users_path).ok();
        let mut users: Users = if let Some(text) = &users_text {
            let users: Users = match toml::from_str(text) {
                Ok(config) => config,
                Err(err) => {
                    let error = Error::config(text, err);
                    error!("failed to load {}: {}", users_path.display(), error);
                    return Err(error);
                }
            };
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

        let config_and_users = ConfigAndUsers {
            config,
            users,
            config_path: config_path.to_owned(),
            users_path: users_path.to_owned(),
            config_text,
            users_text,
        };

        Ok(config_and_users)
    }

    pub fn check(&mut self) -> Result<(), Error> {
        self.config.check();
        self.users.check(&self.config);
        self.validate_server_auth()?;
        Ok(())
    }

    fn validate_server_auth(&self) -> Result<(), Error> {
        let is_external_identity = self
            .users
            .users
            .iter()
            .any(|user| user.is_external_identity());

        if !is_external_identity {
            return Ok(());
        }

        if self.config.general.passthrough_auth != PassthroughAuth::Disabled {
            return Err(Error::ParseError(
                "\"passthrough_auth\" must be \"disabled\" when any user has \"server_auth = \\\"rds_iam\\\"\", \"server_auth = \\\"azure_workload_identity\\\"\" or \"server_auth = \\\"vault\\\"\"".into(),
            ));
        }

        if self.config.general.tls_verify == TlsVerifyMode::Disabled {
            return Err(Error::ParseError(
                "\"tls_verify\" cannot be \"disabled\" when any user has \"server_auth = \\\"rds_iam\\\"\", \"server_auth = \\\"azure_workload_identity\\\"\" or \"server_auth = \\\"vault\\\"\"".into(),
            ));
        }

        Ok(())
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
            config_text: None,
            users_text: None,
        }
    }
}

/// Configuration.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// General settings are relevant to the operations of the pooler itself, or apply to all database pools.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/
    #[serde(default)]
    pub general: General,

    /// Controls PgDog's automatic SQL rewrites for sharded databases. It affects sharding key updates and multi-tuple inserts.
    ///
    /// **Note:** Consider enabling two-phase commit when either feature is set to `rewrite`. Without it, rewrites are committed shard-by-shard and can leave partial changes if a transaction fails.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/rewrite/
    #[serde(default)]
    pub rewrite: Rewrite,

    /// PgDog speaks the Postgres protocol which, underneath, uses TCP. Optimal TCP settings are necessary to quickly recover from database incidents.
    ///
    /// **Note:** Not all networks support or play well with TCP keep-alives. If you see an increased number of dropped connections after enabling these settings, you may have to disable them.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/network/
    #[serde(default)]
    pub tcp: Tcp,

    /// Multi-tenant isolation settings.
    pub multi_tenant: Option<MultiTenant>,

    /// Database settings configure which databases PgDog is managing. This is a TOML list of hosts, ports, and other settings like database roles (primary or replica).
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/databases/
    #[serde(default)]
    pub databases: Vec<Database>,

    /// [Plugins](https://docs.pgdog.dev/features/plugins/) are dynamically loaded at PgDog startup. These settings control which plugins are loaded.
    ///
    /// **Note:** Plugins can only be configured at PgDog startup. They cannot be changed after the process is running.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/plugins/
    #[serde(default)]
    pub plugins: Vec<Plugin>,

    /// Admin database settings control access to the [admin](https://docs.pgdog.dev/administration/) database which contains real time statistics about internal operations of PgDog.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/admin/
    #[serde(default)]
    #[schemars(default = "crate::users::Admin::schemars_default_stub")]
    pub admin: Admin,

    /// To detect and route queries with sharding keys, PgDog expects the sharded column to be specified in the configuration.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/
    #[serde(default)]
    pub sharded_tables: Vec<ShardedTableConfig>,

    /// Queries routed manually to a single shard.
    #[serde(default)]
    pub manual_queries: Vec<ManualQuery>,

    /// Omnisharded tables are tables that contain the same data on all shards. This is useful for storing relatively static metadata used in joins or data that doesn't fit the sharding schema of the database, e.g., list of countries, global settings, list of blocked IPs, etc.
    ///
    /// **Note:** Unless explicitly configured as sharded tables, all tables default to omnisharded status, which makes configuration simpler, and doesn't require explicitly enumerating all tables in `pgdog.toml`.
    ///
    /// https://docs.pgdog.dev/features/sharding/omnishards/
    #[serde(default)]
    pub omnisharded_tables: Vec<OmnishardedTables>,

    /// Explicit sharding key mappings.
    #[serde(default)]
    pub sharded_mappings: Vec<ShardedMappingDeprecated>,

    /// [Schema-based sharding](https://docs.pgdog.dev/features/sharding/sharding-functions/#schema-based-sharding) places data from tables in different Postgres schemas on their own shards.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_schemas/
    #[serde(default)]
    pub sharded_schemas: Vec<ShardedSchema>,

    /// Replica lag configuration.
    #[serde(default, deserialize_with = "ReplicaLag::deserialize_optional")]
    pub replica_lag: Option<ReplicaLag>,

    /// Replication config.
    #[serde(default)]
    pub replication: Replication,

    /// [Mirroring](https://docs.pgdog.dev/features/mirroring/) settings configure traffic mirroring between two databases. When enabled, query traffic is copied from the source database to the destination database, in real time.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/mirroring/
    #[serde(default)]
    pub mirroring: Vec<Mirroring>,

    /// Memory settings control buffer sizes used by PgDog for network I/O and task execution.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/memory/
    #[serde(default)]
    pub memory: Memory,

    /// OpenTelemetry push exporter settings.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/otel/
    #[serde(default)]
    pub otel: Otel,

    /// HashiCorp Vault settings, required for users configured with `server_auth = "vault"`.
    pub vault: Option<Vault>,

    /// Query parser levels per-database.
    #[serde(default)]
    pub query_parsers: Vec<QueryParser>,
}

impl Config {
    /// Organize all databases by name for quicker retrieval.
    pub fn databases(&self) -> HashMap<String, Vec<Vec<EnumeratedDatabase>>> {
        let mut databases = HashMap::new();
        for (number, database) in self.databases.iter().enumerate() {
            let entry = databases
                .entry(database.name.clone())
                .or_insert_with(Vec::new);
            while entry.len() <= database.shard {
                entry.push(vec![]);
            }
            entry
                .get_mut(database.shard)
                .unwrap()
                .push(EnumeratedDatabase {
                    number,
                    database: database.clone(),
                });
        }
        databases
    }

    pub fn omnisharded_tables(&self) -> HashMap<String, Vec<OmnishardedTable>> {
        let mut tables = HashMap::new();

        for table in &self.omnisharded_tables {
            let entry = tables
                .entry(table.database.clone())
                .or_insert_with(Vec::new);
            for t in &table.tables {
                entry.push(OmnishardedTable {
                    name: t.clone(),
                    sticky_routing: table.sticky,
                });
            }
        }

        let databases = self
            .databases
            .iter()
            .map(|database| database.name.clone())
            .collect::<HashSet<_>>();

        // Automatically configure system catalogs
        // as omnisharded.
        if self.general.system_catalogs != SystemCatalogsBehavior::Sharded {
            let sticky_routing = matches!(
                self.general.system_catalogs,
                SystemCatalogsBehavior::OmnishardedSticky
            );
            for database in databases {
                let entry = tables.entry(database).or_insert_with(Vec::new);

                for table in system_catalogs() {
                    if !entry.iter().any(|t| t.name == *table) {
                        entry.push(OmnishardedTable {
                            name: table.to_string(),
                            sticky_routing,
                        });
                    }
                }
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
    pub fn sharded_mappings(&self) -> IndexMap<ShardedMappingKey, Vec<ShardedMappingDeprecated>> {
        let mut mappings = IndexMap::new();

        for mapping in &self.sharded_mappings {
            let mapping = mapping.clone();
            let entry = mappings
                .entry(ShardedMappingKey {
                    database: mapping.database.clone(),
                    column: mapping.column.clone(),
                    table: mapping.table.clone(),
                })
                .or_insert_with(Vec::new);
            entry.push(mapping);
        }

        mappings
    }

    pub fn check(&mut self) {
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

        struct Check {
            pooler_mode: Option<PoolerMode>,
            role: Role,
            role_warned: bool,
            parser_warned: bool,
            mirror_parser_warned: bool,
            have_replicas: bool,
            have_primary: bool,
            have_auto: bool,
            sharded: bool,
        }

        // Check identical configs.
        let mut checks = HashMap::<String, Check>::new();
        for database in &self.databases {
            if let Some(existing) = checks.get_mut(&database.name) {
                if existing.pooler_mode != database.pooler_mode {
                    warn!(
                        "database \"{}\" (shard={}, role={}) has a different \"pooler_mode\" setting, ignoring",
                        database.name, database.shard, database.role,
                    );
                }
                let auto = existing.role == Role::Auto || database.role == Role::Auto;
                if auto && existing.role != database.role && !existing.role_warned {
                    warn!(
                        r#"database "{}" has a mix of auto and specific roles, automatic role detection will be disabled"#,
                        database.name
                    );
                    existing.role_warned = true;
                }
                if !existing.have_replicas {
                    existing.have_replicas = database.role == Role::Replica;
                }
                if !existing.have_primary {
                    existing.have_primary = database.role == Role::Primary;
                }
                if !existing.have_auto {
                    existing.have_auto = database.role == Role::Auto;
                }
                if !existing.sharded {
                    existing.sharded = database.shard > 0;
                }

                if (existing.sharded || (existing.have_replicas && existing.have_primary))
                    && self.general.query_parser == QueryParserLevel::Off
                    && !existing.parser_warned
                {
                    existing.parser_warned = true;
                    warn!(
                        r#"database "{}" may need the query parser for load balancing/sharding, but it's disabled"#,
                        database.name
                    );
                }
            } else {
                checks.insert(
                    database.name.clone(),
                    Check {
                        pooler_mode: database.pooler_mode,
                        role: database.role,
                        role_warned: false,
                        parser_warned: false,
                        mirror_parser_warned: false,
                        have_primary: database.role == Role::Primary,
                        have_replicas: database.role == Role::Replica,
                        have_auto: database.role == Role::Auto,
                        sharded: database.shard > 0,
                    },
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
            PassthroughAuth::Enabled if !self.general.tls_client_required => {
                warn!(
                    "consider setting \"tls_client_required\" while \"passthrough_auth\" is enabled to prevent clients from exposing plaintext passwords"
                );
            }
            PassthroughAuth::EnabledPlain => {
                warn!(
                    "\"passthrough_auth\" is set to \"plain\", network traffic may expose plaintext passwords"
                )
            }
            _ => (),
        }

        if !self.general.two_phase_commit && self.rewrite.enabled {
            if self.rewrite.shard_key == RewriteMode::Rewrite {
                warn!(
                    r#"rewrite.shard_key = "rewrite" may apply non-atomic sharding key rewrites; enabling "two_phase_commit" is strongly recommended"#
                );
            }

            if self.rewrite.split_inserts == RewriteMode::Rewrite {
                warn!(
                    r#"rewrite.split_inserts = "rewrite" may commit partial multi-row inserts; enabling "two_phase_commit" is strongly recommended"#
                );
            }
        }

        for mirror in &self.mirroring {
            if mirror.level == MirroringLevel::All {
                continue;
            }
            if let Some(check) = checks.get_mut(&mirror.source_db) {
                if check.mirror_parser_warned {
                    continue;
                }
                let parser_enabled = match self.general.query_parser {
                    QueryParserLevel::On => true,
                    QueryParserLevel::Off
                    | QueryParserLevel::SessionControl
                    | QueryParserLevel::SessionControlAndLocks => false,
                    QueryParserLevel::Auto => check.have_replicas || check.sharded,
                };
                if !parser_enabled {
                    check.mirror_parser_warned = true;
                    warn!(
                        r#"mirroring from "{}" with level "{}" requires the query parser to classify statements, but it won't be enabled, set query_parser = "on""#,
                        mirror.source_db, mirror.level
                    );
                }
            }
        }

        for (database, check) in &checks {
            if !check.have_replicas
                && self.general.read_write_split == ReadWriteSplit::ExcludePrimary
                && !check.have_auto
            {
                warn!(
                    r#"database "{}" has no replicas and "read_write_split" is set to "{}": read queries will be rejected"#,
                    database, self.general.read_write_split
                );
            }

            if self.general.lsn_checks_enabled() && !check.have_auto && !check.have_replicas {
                warn!(
                    r#"database "{}" has no replicas and LSN checks are enabled: PgDog will query databases for their LSNs unnecessarily"#,
                    database
                )
            }

            if !self.general.lsn_checks_enabled() && check.have_auto {
                warn!(
                    r#"database "{}" has a role set to "auto" but LSN checks are disabled: this disables automatic role detection"#,
                    database
                );
            }
        }

        if self.general.query_parser_enabled {
            warn!(r#""query_parser_enabled" is deprecated, use "query_parser" = "on" instead"#);
            self.general.query_parser = QueryParserLevel::On;
        }

        let raw_query_parser = self.general.query_parser_engine == QueryParserEngine::PgQueryRaw
            || self
                .query_parsers
                .iter()
                .any(|query_parser| query_parser.engine == QueryParserEngine::PgQueryRaw);

        if raw_query_parser && self.memory.stack_size < 32 * 1024 * 1024 {
            self.memory.stack_size = 32 * 1024 * 1024;
            warn!(
                r#""pg_query_raw" parser engine requires a large thread stack, setting it to 32MiB for each Tokio worker"#
            );
        }

        if !self.sharded_mappings.is_empty() {
            warn!(
                "`[[sharded_mappings]]` config is deprecated, use `[[sharded_tables.mapping]]` instead"
            )
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
                level: m.level,
            })
    }

    /// Get all mirroring configurations mapped by source database.
    pub fn mirroring_by_source(&self) -> HashMap<String, Vec<(String, MirrorConfig)>> {
        let mut result = HashMap::new();

        for mirror in &self.mirroring {
            let config = MirrorConfig {
                queue_length: mirror.queue_length.unwrap_or(self.general.mirror_queue),
                exposure: mirror.exposure.unwrap_or(self.general.mirror_exposure),
                level: mirror.level,
            };

            result
                .entry(mirror.source_db.clone())
                .or_insert_with(Vec::new)
                .push((mirror.destination_db.clone(), config));
        }

        result
    }

    /// Swap database configs between `source` and `destination`.
    /// Uses tmp pattern: source -> tmp, destination -> source, tmp -> destination.
    pub fn cutover(&mut self, source: &str, destination: &str) {
        // force setting the database name on cutover to make sure
        // the proper database_name is present after the swap.
        for db in self.databases.iter_mut() {
            db.database_name = db.database_name.take().or(Some(db.name.clone()));
        }

        let tmp = format!("__tmp_{}__", random_string(12));

        crate::swap_field!(self.databases.iter_mut(), name, source, destination, tmp);
        crate::swap_field!(
            self.sharded_mappings.iter_mut(),
            database,
            source,
            destination,
            tmp
        );
        crate::swap_field!(
            self.sharded_tables.iter_mut(),
            database,
            source,
            destination,
            tmp
        );
        crate::swap_field!(
            self.omnisharded_tables.iter_mut(),
            database,
            source,
            destination,
            tmp
        );
        crate::swap_field!(
            self.mirroring.iter_mut(),
            source_db,
            source,
            destination,
            tmp
        );
        crate::swap_field!(
            self.mirroring.iter_mut(),
            destination_db,
            source,
            destination,
            tmp
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FlexibleType, ShardedMappingConfig, ShardedMappingList, ShardedMappingRange};
    use crate::{PoolerMode, PreparedStatements, ServerAuth};

    use std::io::Write;
    use std::time::Duration;
    use tempfile::NamedTempFile;

    #[test]
    fn test_basic() {
        let pgdog_source = r#"
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

        let users_source =
            "[[users]]\nname = \"alice\"\ndatabase = \"production\"\npassword = \"pw\"\n";

        let mut pgdog_file = NamedTempFile::new().unwrap();
        pgdog_file.write_all(pgdog_source.as_bytes()).unwrap();
        pgdog_file.flush().unwrap();

        let mut users_file = NamedTempFile::new().unwrap();
        users_file.write_all(users_source.as_bytes()).unwrap();
        users_file.flush().unwrap();

        let loaded = ConfigAndUsers::load(pgdog_file.path(), users_file.path()).unwrap();
        let config = &loaded.config;

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
        assert_eq!(config.multi_tenant.as_ref().unwrap().column, "tenant_id");

        // Users parsed from the second file.
        assert_eq!(loaded.users.users.len(), 1);
        assert_eq!(loaded.users.users[0].name, "alice");
        assert_eq!(loaded.users.users[0].database, "production");
        assert_eq!(loaded.users.users[0].password.as_deref(), Some("pw"));

        // Raw text captured verbatim from the files that were read.
        assert_eq!(loaded.config_text.as_deref(), Some(pgdog_source));
        assert_eq!(loaded.users_text.as_deref(), Some(users_source));

        // Paths point at the files we loaded.
        assert_eq!(loaded.config_path, pgdog_file.path());
        assert_eq!(loaded.users_path, users_file.path());
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
        assert!(
            config
                .get_mirroring_config("source_db", "non_existent")
                .is_none()
        );
    }

    #[test]
    fn test_admin_override_from_users_toml() {
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

        let config_and_users = ConfigAndUsers::load(pgdog_file.path(), users_file.path()).unwrap();

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

        let config_and_users = ConfigAndUsers::load(pgdog_file.path(), users_file.path()).unwrap();

        assert_eq!(config_and_users.config.admin.name, "users_admin");
        assert_eq!(config_and_users.config.admin.user, "users_admin_user");
        assert_eq!(
            config_and_users.config.admin.password,
            "users_admin_password"
        );
        assert!(config_and_users.users.admin.is_none());
    }

    #[test]
    fn test_omnisharded_tables() {
        let source = r#"
[general]
host = "0.0.0.0"
port = 6432
system_catalogs = "sharded"

[[databases]]
name = "db1"
host = "127.0.0.1"
port = 5432

[[databases]]
name = "db2"
host = "127.0.0.1"
port = 5433

[[omnisharded_tables]]
database = "db1"
tables = ["table_a", "table_b"]

[[omnisharded_tables]]
database = "db1"
tables = ["table_c"]
sticky = true

[[omnisharded_tables]]
database = "db2"
tables = ["table_x"]
"#;

        let config: Config = toml::from_str(source).unwrap();

        assert_eq!(config.omnisharded_tables.len(), 3);

        let tables = config.omnisharded_tables();

        assert_eq!(tables.len(), 2);

        let db1_tables = tables.get("db1").unwrap();
        assert_eq!(db1_tables.len(), 3);
        assert_eq!(db1_tables[0].name, "table_a");
        assert!(!db1_tables[0].sticky_routing);
        assert_eq!(db1_tables[1].name, "table_b");
        assert!(!db1_tables[1].sticky_routing);
        assert_eq!(db1_tables[2].name, "table_c");
        assert!(db1_tables[2].sticky_routing);

        let db2_tables = tables.get("db2").unwrap();
        assert_eq!(db2_tables.len(), 1);
        assert_eq!(db2_tables[0].name, "table_x");
        assert!(!db2_tables[0].sticky_routing);
    }

    #[test]
    fn test_omnisharded_tables_system_catalogs() {
        // Test with system_catalogs_omnisharded = true
        let source_enabled = r#"
[general]
host = "0.0.0.0"
port = 6432
system_catalogs = "omnisharded_sticky"

[[databases]]
name = "db1"
host = "127.0.0.1"
port = 5432

[[omnisharded_tables]]
database = "db1"
tables = ["my_table"]
"#;

        let config: Config = toml::from_str(source_enabled).unwrap();
        let tables = config.omnisharded_tables();
        let db1_tables = tables.get("db1").unwrap();

        // Should include my_table plus system catalogs
        assert!(db1_tables.iter().any(|t| t.name == "my_table"));
        assert!(db1_tables.iter().any(|t| t.name == "pg_class"));
        assert!(db1_tables.iter().any(|t| t.name == "pg_attribute"));
        assert!(db1_tables.iter().any(|t| t.name == "pg_namespace"));
        assert!(db1_tables.iter().any(|t| t.name == "pg_type"));

        // System catalogs should have sticky_routing = true
        let pg_class = db1_tables.iter().find(|t| t.name == "pg_class").unwrap();
        assert!(pg_class.sticky_routing);

        // Test with system_catalogs = "sharded" (no omnisharding)
        let source_disabled = r#"
[general]
host = "0.0.0.0"
port = 6432
system_catalogs = "sharded"

[[databases]]
name = "db1"
host = "127.0.0.1"
port = 5432

[[omnisharded_tables]]
database = "db1"
tables = ["my_table"]
"#;

        let config: Config = toml::from_str(source_disabled).unwrap();
        let tables = config.omnisharded_tables();
        let db1_tables = tables.get("db1").unwrap();

        // Should only include my_table, no system catalogs
        assert_eq!(db1_tables.len(), 1);
        assert_eq!(db1_tables[0].name, "my_table");
        assert!(!db1_tables.iter().any(|t| t.name == "pg_class"));
        assert!(!db1_tables.iter().any(|t| t.name == "pg_attribute"));
    }

    #[test]
    fn test_cutover_swaps_database_configs() {
        let mut config = Config {
            databases: vec![
                Database {
                    name: "source_db".to_string(),
                    host: "source-host".to_string(),
                    port: 5432,
                    role: Role::Primary,
                    ..Default::default()
                },
                Database {
                    name: "destination_db".to_string(),
                    host: "destination-host".to_string(),
                    port: 5433,
                    role: Role::Primary,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        // After cutover: looking up source_db returns destination's config
        config.cutover("source_db", "destination_db");

        assert_eq!(config.databases.len(), 2);

        // source_db should now have destination's config (host, port)
        let source = config
            .databases
            .iter()
            .find(|d| d.name == "source_db")
            .unwrap();
        assert_eq!(
            source.host, "destination-host",
            "source_db should now have destination's host after cutover"
        );
        assert_eq!(
            source.port, 5433,
            "source_db should now have destination's port after cutover"
        );

        // destination_db should now have source's config (host, port)
        let destination = config
            .databases
            .iter()
            .find(|d| d.name == "destination_db")
            .unwrap();
        assert_eq!(
            destination.host, "source-host",
            "destination_db should now have source's host after cutover"
        );
        assert_eq!(
            destination.port, 5432,
            "destination_db should now have source's port after cutover"
        );
    }

    #[test]
    fn test_cutover_preserves_physical_database_name() {
        // `source_db` relies on the default (physical db == cluster name);
        // `destination_db` sets an explicit `database_name`. After cutover the
        // physical target of each entry must be unchanged — only the logical
        // cluster name swaps. Regression test for entries connecting to a
        // nonexistent database after a name swap.
        let mut config = Config {
            databases: vec![
                Database {
                    name: "source_db".to_string(),
                    host: "source-host".to_string(),
                    port: 5432,
                    database_name: None,
                    ..Default::default()
                },
                Database {
                    name: "destination_db".to_string(),
                    host: "destination-host".to_string(),
                    port: 5433,
                    database_name: Some("real_dest".to_string()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        config.cutover("source_db", "destination_db");

        // The entry now named `source_db` carries destination's config; its
        // physical target must remain destination's database.
        let source = config
            .databases
            .iter()
            .find(|d| d.name == "source_db")
            .unwrap();
        assert_eq!(source.host, "destination-host");
        assert_eq!(source.database_name.as_deref(), Some("real_dest"));

        // The entry now named `destination_db` carries source's config. Source
        // relied on the default, so its physical target was pinned to the old
        // name (`source_db`) — not silently changed to `destination_db`.
        let destination = config
            .databases
            .iter()
            .find(|d| d.name == "destination_db")
            .unwrap();
        assert_eq!(destination.host, "source-host");
        assert_eq!(destination.database_name.as_deref(), Some("source_db"));
    }

    #[test]
    fn test_cutover_visual() {
        let before = r#"
[[databases]]
name = "source_db"
host = "source-host-0"
port = 5432
role = "primary"
shard = 0

[[databases]]
name = "source_db"
host = "source-host-0-replica"
port = 5432
role = "replica"
shard = 0

[[databases]]
name = "source_db"
host = "source-host-1"
port = 5432
role = "primary"
shard = 1

[[databases]]
name = "source_db"
host = "source-host-1-replica"
port = 5432
role = "replica"
shard = 1

[[databases]]
name = "destination_db"
host = "destination-host-0"
port = 5433
role = "primary"
shard = 0

[[databases]]
name = "destination_db"
host = "destination-host-0-replica"
port = 5433
role = "replica"
shard = 0

[[databases]]
name = "destination_db"
host = "destination-host-1"
port = 5433
role = "primary"
shard = 1

[[databases]]
name = "destination_db"
host = "destination-host-1-replica"
port = 5433
role = "replica"
shard = 1

[[sharded_tables]]
database = "source_db"
name = "users"
column = "id"

[[sharded_tables]]
database = "destination_db"
name = "users"
column = "id"

[[mirroring]]
source_db = "source_db"
destination_db = "destination_db"
"#;

        // After name swap: elements stay in place, only names change
        // Original source_db entries become destination_db (keeping source's host)
        // Original destination_db entries become source_db (keeping destination's host)
        let expected_after = r#"
[[databases]]
name = "destination_db"
database_name = "source_db"
host = "source-host-0"
port = 5432
role = "primary"
shard = 0

[[databases]]
name = "destination_db"
database_name = "source_db"
host = "source-host-0-replica"
port = 5432
role = "replica"
shard = 0

[[databases]]
name = "destination_db"
database_name = "source_db"
host = "source-host-1"
port = 5432
role = "primary"
shard = 1

[[databases]]
name = "destination_db"
database_name = "source_db"
host = "source-host-1-replica"
port = 5432
role = "replica"
shard = 1

[[databases]]
name = "source_db"
database_name = "destination_db"
host = "destination-host-0"
port = 5433
role = "primary"
shard = 0

[[databases]]
name = "source_db"
database_name = "destination_db"
host = "destination-host-0-replica"
port = 5433
role = "replica"
shard = 0

[[databases]]
name = "source_db"
database_name = "destination_db"
host = "destination-host-1"
port = 5433
role = "primary"
shard = 1

[[databases]]
name = "source_db"
database_name = "destination_db"
host = "destination-host-1-replica"
port = 5433
role = "replica"
shard = 1

[[sharded_tables]]
database = "destination_db"
name = "users"
column = "id"

[[sharded_tables]]
database = "source_db"
name = "users"
column = "id"

[[mirroring]]
source_db = "destination_db"
destination_db = "source_db"
"#;

        let mut config: Config = toml::from_str(before).unwrap();
        config.cutover("source_db", "destination_db");

        let expected: Config = toml::from_str(expected_after).unwrap();

        assert_eq!(config.databases, expected.databases);
        assert_eq!(config.sharded_tables, expected.sharded_tables);
        assert_eq!(config.mirroring, expected.mirroring);
    }

    #[test]
    fn test_cutover_backup_roundtrip() {
        let original_toml = r#"
[[databases]]
name = "source_db"
host = "source-host"
port = 5432
role = "primary"
shard = 0

[[databases]]
name = "destination_db"
host = "destination-host"
port = 5433
role = "primary"
shard = 0
"#;

        // Parse original config
        let original: Config = toml::from_str(original_toml).unwrap();

        // Simulate backup: serialize original to TOML
        let backup_toml = toml::to_string_pretty(&original).unwrap();

        // Perform cutover
        let mut config = original.clone();
        config.cutover("source_db", "destination_db");

        // Serialize cutover result (what would be written to disk)
        let new_toml = toml::to_string_pretty(&config).unwrap();

        // Verify backup can be parsed back and matches original
        let restored_backup: Config = toml::from_str(&backup_toml).unwrap();
        assert_eq!(restored_backup.databases, original.databases);

        // Verify new config can be parsed back and has swapped values
        let restored_new: Config = toml::from_str(&new_toml).unwrap();

        // After cutover: source_db should have destination's host
        let source = restored_new
            .databases
            .iter()
            .find(|d| d.name == "source_db")
            .unwrap();
        assert_eq!(source.host, "destination-host");
        assert_eq!(source.port, 5433);

        // After cutover: destination_db should have source's host
        let dest = restored_new
            .databases
            .iter()
            .find(|d| d.name == "destination_db")
            .unwrap();
        assert_eq!(dest.host, "source-host");
        assert_eq!(dest.port, 5432);
    }

    #[test]
    fn test_rds_iam_rejects_passthrough_auth() {
        let mut config = ConfigAndUsers::default();
        config.config.general.passthrough_auth = PassthroughAuth::EnabledPlain;
        config.config.general.tls_verify = TlsVerifyMode::VerifyFull;
        config.users.users.push(crate::User {
            name: "alice".into(),
            database: "db".into(),
            password: Some("secret".into()),
            server_auth: ServerAuth::RdsIam,
            ..Default::default()
        });

        let err = config.check().unwrap_err().to_string();
        assert!(err.contains("passthrough_auth"));
        assert!(err.contains("rds_iam"));
    }

    #[test]
    fn test_rds_iam_rejects_tls_verify_disabled() {
        let mut config = ConfigAndUsers::default();
        config.config.general.tls_verify = TlsVerifyMode::Disabled;
        config.config.general.passthrough_auth = PassthroughAuth::Disabled;
        config.users.users.push(crate::User {
            name: "alice".into(),
            database: "db".into(),
            password: Some("secret".into()),
            server_auth: ServerAuth::RdsIam,
            ..Default::default()
        });

        let err = config.check().unwrap_err().to_string();
        assert!(err.contains("tls_verify"));
        assert!(err.contains("rds_iam"));
    }

    #[test]
    fn test_azure_workload_identity_rejects_passthrough_auth() {
        let mut config = ConfigAndUsers::default();
        config.config.general.passthrough_auth = PassthroughAuth::EnabledPlain;
        config.config.general.tls_verify = TlsVerifyMode::VerifyFull;
        config.users.users.push(crate::User {
            name: "alice".into(),
            database: "db".into(),
            password: Some("secret".into()),
            server_auth: ServerAuth::AzureWorkloadIdentity,
            ..Default::default()
        });

        let err = config.check().unwrap_err().to_string();
        assert!(err.contains("passthrough_auth"));
        assert!(err.contains("azure_workload_identity"));
    }

    #[test]
    fn test_azure_workload_identity_rejects_tls_verify_disabled() {
        let mut config = ConfigAndUsers::default();
        config.config.general.tls_verify = TlsVerifyMode::Disabled;
        config.config.general.passthrough_auth = PassthroughAuth::Disabled;
        config.users.users.push(crate::User {
            name: "alice".into(),
            database: "db".into(),
            password: Some("secret".into()),
            server_auth: ServerAuth::AzureWorkloadIdentity,
            ..Default::default()
        });

        let err = config.check().unwrap_err().to_string();
        assert!(err.contains("tls_verify"));
        assert!(err.contains("azure_workload_identity"));
    }

    #[test]
    fn test_sharded_table_inline_mapping() {
        let source = r#"
[[sharded_tables]]
database = "db"
column = "user_id"
mapping = [{values = [1, 2, 3], shard = 0}, {values = [4, 5, 6], shard = 1}, {shard = 2}]

[[sharded_tables]]
database = "db"
column = "order_id"
mapping = [{start = 0, end = 1000, shard = 0}, {start = 1000, end = 2000, shard = 1}]

[[sharded_tables]]
database = "db"
column = "tenant_uuid"
data_type = "uuid"
mapping = [{values = ["00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"], shard = 0}]

[[sharded_tables]]
database = "db"
column = "tenant_slug"
data_type = "varchar"
mapping = [{values = ["alpha", "beta"], shard = 0}, {shard = 1}]

[[sharded_tables]]
database = "db"
column = "legacy_id"
"#;
        assert_mapping_config(&toml::from_str(source).unwrap());
    }

    #[test]
    fn test_sharded_table_separate_mapping() {
        let source = r#"
[[sharded_tables]]
database = "db"
column = "user_id"

[[sharded_tables.mapping]]
values = [1, 2, 3]
shard = 0

[[sharded_tables.mapping]]
values = [4, 5, 6]
shard = 1

[[sharded_tables.mapping]]
shard = 2

[[sharded_tables]]
database = "db"
column = "order_id"

[[sharded_tables.mapping]]
start = 0
end = 1000
shard = 0

[[sharded_tables.mapping]]
start = 1000
end = 2000
shard = 1

[[sharded_tables]]
database = "db"
column = "tenant_uuid"
data_type = "uuid"

[[sharded_tables.mapping]]
values = ["00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"]
shard = 0

[[sharded_tables]]
database = "db"
column = "tenant_slug"
data_type = "varchar"

[[sharded_tables.mapping]]
values = ["alpha", "beta"]
shard = 0

[[sharded_tables.mapping]]
shard = 1

[[sharded_tables]]
database = "db"
column = "legacy_id"
"#;
        assert_mapping_config(&toml::from_str(source).unwrap());
    }

    fn assert_mapping_config(config: &Config) {
        assert_eq!(config.sharded_tables.len(), 5);

        assert_eq!(
            config.sharded_tables[0].mapping,
            Some(vec![
                ShardedMappingConfig::List(ShardedMappingList {
                    values: vec![
                        FlexibleType::Integer(1),
                        FlexibleType::Integer(2),
                        FlexibleType::Integer(3)
                    ],
                    shard: 0
                }),
                ShardedMappingConfig::List(ShardedMappingList {
                    values: vec![
                        FlexibleType::Integer(4),
                        FlexibleType::Integer(5),
                        FlexibleType::Integer(6)
                    ],
                    shard: 1
                }),
                ShardedMappingConfig::Default { shard: 2 },
            ])
        );

        assert_eq!(
            config.sharded_tables[1].mapping,
            Some(vec![
                ShardedMappingConfig::Range(ShardedMappingRange {
                    start: Some(FlexibleType::Integer(0)),
                    end: Some(FlexibleType::Integer(1000)),
                    shard: 0
                }),
                ShardedMappingConfig::Range(ShardedMappingRange {
                    start: Some(FlexibleType::Integer(1000)),
                    end: Some(FlexibleType::Integer(2000)),
                    shard: 1
                }),
            ])
        );

        assert_eq!(
            config.sharded_tables[2].mapping,
            Some(vec![ShardedMappingConfig::List(ShardedMappingList {
                values: vec![
                    FlexibleType::Uuid("00000000-0000-0000-0000-000000000001".parse().unwrap()),
                    FlexibleType::Uuid("00000000-0000-0000-0000-000000000002".parse().unwrap()),
                ],
                shard: 0,
            }),])
        );

        assert_eq!(
            config.sharded_tables[3].mapping,
            Some(vec![
                ShardedMappingConfig::List(ShardedMappingList {
                    values: vec![
                        FlexibleType::String("alpha".into()),
                        FlexibleType::String("beta".into())
                    ],
                    shard: 0
                }),
                ShardedMappingConfig::Default { shard: 1 },
            ])
        );

        assert_eq!(config.sharded_tables[4].mapping, None);
    }
}
