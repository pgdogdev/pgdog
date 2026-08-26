//! A collection of replicas and a primary.

use futures::future::try_join_all;
use parking_lot::Mutex;
use pgdog_config::{
    LoadSchema, PreparedStatements, QueryParser, QueryParserLevel, Rewrite, RewriteMode,
    users::PasswordKind,
};
use std::{sync::Arc, time::Duration};

use crate::backend::schema::SchemaCache;
use crate::backend::server::ServerRequest;
use crate::frontend::router::sharding::ShardedTable;
use crate::{
    backend::{
        Schema, ShardedTables, databases::User as DatabaseUser, replication::ShardedSchemas,
    },
    config::{
        ConnectionRecovery, MultiTenant, PoolerMode, ReadWriteSplit, ReadWriteStrategy, User,
    },
    frontend::{ClientRequest, RegexParser, router::round_robin},
    net::{bind::Parameter as BindParameter, messages::DataRow, messages::FrontendPid},
};

use super::{
    Address, CanonicalOids, ClusterMetrics, Config, Error, Guard, Request, Shard, ShardConfig,
};
use crate::config::LoadBalancingStrategy;
use launch::Readiness;

pub(crate) mod launch;
pub(crate) mod schema_loader;

pub(crate) use schema_loader::SchemaLoader;

#[derive(Clone, Debug, Default)]
/// Database configuration.
pub struct PoolConfig {
    /// Database address.
    pub(crate) address: Address,
    /// Pool settings.
    pub(crate) config: Config,
}

/// A collection of sharded replicas and primaries
/// belonging to the same database cluster.
/// Mapped to a singular `User`, see "databases: HashMap<User, Cluster>" in backend/databases.rs
#[derive(Clone, Debug)]
pub struct Cluster {
    identifier: Arc<DatabaseUser>,
    shards: Vec<Shard>,
    passwords: Vec<PasswordKind>,
    pooler_mode: PoolerMode,
    sharded_tables: ShardedTables,
    sharded_schemas: ShardedSchemas,
    multi_tenant: Option<MultiTenant>,
    rw_strategy: ReadWriteStrategy,
    rw_split: ReadWriteSplit,
    schema_admin: bool,
    stats: Arc<Mutex<ClusterMetrics>>,
    cross_shard_disabled: bool,
    two_phase_commit: bool,
    two_phase_commit_auto: bool,
    readiness: Arc<Readiness>,
    rewrite: Rewrite,
    prepared_statements: PreparedStatements,
    dry_run: bool,
    expanded_explain: bool,
    query_parser: QueryParserLevel,
    client_connection_recovery: ConnectionRecovery,
    log_min_duration_parse: Option<Duration>,
    log_query_sample_length: usize,
    reload_schema_on_ddl: bool,
    load_schema: LoadSchema,
    resharding_parallel_copies: usize,
    resharding_copy_retry_max_attempts: usize,
    resharding_copy_retry_min_delay: Duration,
    resharding_replication_retry_max_attempts: usize,
    resharding_replication_retry_min_delay: Duration,
    sharding_lookup_timeout: Duration,
    regex_parser: RegexParser,
    identity: Option<String>,
    tls_client_certificate_required: bool,
    #[debug(skip)]
    schema_loader: Box<dyn SchemaLoader>,
    canonical_oids: Option<Arc<CanonicalOids>>,
    read_only: bool,
}

/// Bare test clusters carry the same defaults the config would apply,
/// so settings like the lookup timeout come from `pgdog-config` in
/// tests too, not from a zeroed field.
#[cfg(test)]
impl Default for Cluster {
    fn default() -> Self {
        use pgdog_config::General;

        Self {
            identifier: Default::default(),
            shards: Default::default(),
            passwords: Default::default(),
            pooler_mode: Default::default(),
            sharded_tables: Default::default(),
            sharded_schemas: Default::default(),
            multi_tenant: Default::default(),
            rw_strategy: Default::default(),
            rw_split: Default::default(),
            schema_admin: Default::default(),
            stats: Default::default(),
            cross_shard_disabled: Default::default(),
            two_phase_commit: Default::default(),
            two_phase_commit_auto: Default::default(),
            readiness: Default::default(),
            rewrite: Default::default(),
            prepared_statements: Default::default(),
            dry_run: Default::default(),
            expanded_explain: Default::default(),
            query_parser: Default::default(),
            client_connection_recovery: Default::default(),
            log_min_duration_parse: Default::default(),
            log_query_sample_length: Default::default(),
            reload_schema_on_ddl: Default::default(),
            load_schema: Default::default(),
            resharding_parallel_copies: Default::default(),
            resharding_copy_retry_max_attempts: Default::default(),
            resharding_copy_retry_min_delay: Default::default(),
            resharding_replication_retry_max_attempts: Default::default(),
            resharding_replication_retry_min_delay: Default::default(),
            sharding_lookup_timeout: Duration::from_millis(General::sharding_lookup_timeout()),
            regex_parser: Default::default(),
            identity: Default::default(),
            tls_client_certificate_required: Default::default(),
            schema_loader: Default::default(),
            canonical_oids: Default::default(),
            read_only: Default::default(),
        }
    }
}

/// Sharding configuration from the cluster.
#[derive(Debug, Clone, Default)]
pub struct ShardingSchema {
    /// Number of shards.
    pub shards: usize,
    /// Sharded tables.
    pub tables: ShardedTables,
    /// Schemas.
    pub schemas: ShardedSchemas,
    /// Rewrite config.
    pub rewrite: Rewrite,
    pub log_min_duration_parse: Option<Duration>,
    pub log_query_sample_length: usize,
}

impl ShardingSchema {
    pub fn tables(&self) -> &ShardedTables {
        &self.tables
    }
}

#[derive(Debug)]
pub struct ClusterShardConfig {
    pub primary: Option<PoolConfig>,
    pub replicas: Vec<PoolConfig>,
}

impl ClusterShardConfig {
    pub fn pooler_mode(&self) -> PoolerMode {
        // One of these will exist.

        if let Some(ref primary) = self.primary {
            return primary.config.pooler_mode;
        }

        self.replicas
            .first()
            .map(|replica| replica.config.pooler_mode)
            .unwrap_or_default()
    }
}

/// Cluster creation config.
#[derive(Debug)]
pub struct ClusterConfig<'a> {
    name: &'a str,
    shards: &'a [ClusterShardConfig],
    lb_strategy: LoadBalancingStrategy,
    user: &'a str,
    passwords: Vec<PasswordKind>,
    pooler_mode: PoolerMode,
    sharded_tables: ShardedTables,
    multi_tenant: &'a Option<MultiTenant>,
    rw_strategy: ReadWriteStrategy,
    rw_split: ReadWriteSplit,
    schema_admin: bool,
    cross_shard_disabled: bool,
    two_pc: bool,
    two_pc_auto: bool,
    sharded_schemas: ShardedSchemas,
    rewrite: &'a Rewrite,
    prepared_statements: &'a PreparedStatements,
    dry_run: bool,
    expanded_explain: bool,
    query_parser: QueryParserLevel,
    log_min_duration_parse: Option<Duration>,
    log_query_sample_length: usize,
    client_connection_recovery: ConnectionRecovery,
    lsn_check_interval: Duration,
    reload_schema_on_ddl: bool,
    load_schema: LoadSchema,
    resharding_parallel_copies: usize,
    resharding_copy_retry_max_attempts: usize,
    resharding_copy_retry_min_delay: u64,
    resharding_replication_retry_max_attempts: usize,
    resharding_replication_retry_min_delay: u64,
    sharding_lookup_timeout: u64,
    regex_parser_limit: usize,
    pub_sub_enabled: bool,
    identity: &'a Option<String>,
    tls_client_certificate_required: bool,
    schema_cache: SchemaCache,
    canonicalize_oids: bool,
    read_only: bool,
}

impl<'a> ClusterConfig<'a> {
    /// Dependencies for creating a Cluster.
    ///
    /// TODO(lev): This is getting unruly. We may need a struct for a struct :)
    ///
    pub(crate) fn new(
        config: &'a crate::config::Config,
        user: &'a User,
        shards: &'a [ClusterShardConfig],
        sharded_tables: ShardedTables,
        sharded_schemas: ShardedSchemas,
        query_parser: QueryParser,
        schema_cache: SchemaCache,
    ) -> Self {
        let general = &config.general;
        let multi_tenant = config.multi_tenant();
        let rewrite = &config.rewrite;

        let pooler_mode = shards
            .first()
            .map(|shard| shard.pooler_mode())
            .unwrap_or(user.pooler_mode.unwrap_or(general.pooler_mode));

        Self {
            name: &user.database,
            passwords: user.passwords(),
            user: &user.name,
            pooler_mode,
            lb_strategy: general.load_balancing_strategy,
            shards,
            sharded_tables,
            multi_tenant,
            rw_strategy: general.read_write_strategy,
            rw_split: general.read_write_split,
            schema_admin: user.schema_admin,
            cross_shard_disabled: user
                .cross_shard_disabled
                .unwrap_or(general.cross_shard_disabled),
            two_pc: user.two_phase_commit.unwrap_or(general.two_phase_commit),
            two_pc_auto: user
                .two_phase_commit_auto
                .unwrap_or(general.two_phase_commit_auto.unwrap_or(false)), // Disable by default.
            sharded_schemas,
            rewrite,
            prepared_statements: &general.prepared_statements,
            dry_run: general.dry_run,
            expanded_explain: general.expanded_explain,
            query_parser: query_parser.level,
            log_min_duration_parse: general.log_min_duration_parse(),
            log_query_sample_length: general.log_query_sample_length,
            client_connection_recovery: general.client_connection_recovery,
            lsn_check_interval: Duration::from_millis(general.lsn_check_interval),
            reload_schema_on_ddl: general.reload_schema_on_ddl,
            load_schema: general.load_schema,
            resharding_parallel_copies: general.resharding_parallel_copies,
            resharding_copy_retry_max_attempts: general.resharding_copy_retry_max_attempts,
            sharding_lookup_timeout: general.sharding_lookup_timeout,
            resharding_copy_retry_min_delay: general.resharding_copy_retry_min_delay,
            resharding_replication_retry_max_attempts: general
                .resharding_replication_retry_max_attempts,
            resharding_replication_retry_min_delay: general.resharding_replication_retry_min_delay,
            regex_parser_limit: general.regex_parser_limit,
            pub_sub_enabled: general.pub_sub_enabled(),
            identity: &user.identity,
            tls_client_certificate_required: user.tls_client_certificate_required.unwrap_or(true),
            schema_cache,
            canonicalize_oids: general.canonicalize_type_information,
            read_only: user.read_only.unwrap_or(false),
        }
    }
}

impl Cluster {
    /// Create new cluster of shards.
    pub fn new(config: ClusterConfig) -> Self {
        let ClusterConfig {
            name,
            shards,
            lb_strategy,
            user,
            passwords,
            pooler_mode,
            sharded_tables,
            multi_tenant,
            rw_strategy,
            rw_split,
            schema_admin,
            cross_shard_disabled,
            two_pc,
            two_pc_auto,
            sharded_schemas,
            rewrite,
            prepared_statements,
            dry_run,
            expanded_explain,
            query_parser,
            client_connection_recovery,
            lsn_check_interval,
            log_min_duration_parse,
            log_query_sample_length,
            reload_schema_on_ddl,
            load_schema,
            resharding_parallel_copies,
            resharding_copy_retry_max_attempts,
            resharding_copy_retry_min_delay,
            resharding_replication_retry_max_attempts,
            resharding_replication_retry_min_delay,
            sharding_lookup_timeout,
            regex_parser_limit,
            pub_sub_enabled,
            identity,
            tls_client_certificate_required,
            schema_cache,
            canonicalize_oids,
            read_only,
        } = config;

        let identifier = Arc::new(DatabaseUser {
            user: user.to_owned(),
            database: name.to_owned(),
        });
        let canonical_oids = canonicalize_oids.then(|| schema_cache.canonical_oids(name));

        let stats = Arc::new(Mutex::new(ClusterMetrics {
            lookup: sharded_tables.lookup_cache().stats().clone(),
            ..Default::default()
        }));

        Self {
            identifier: identifier.clone(),
            shards: shards
                .iter()
                .enumerate()
                .map(|(number, config)| {
                    Shard::new(ShardConfig {
                        number,
                        primary: config.primary.as_ref(),
                        replicas: &config.replicas,
                        lb_strategy,
                        rw_split,
                        identifier: identifier.clone(),
                        lsn_check_interval,
                        pub_sub_enabled,
                        schema_cache: schema_cache.clone(),
                    })
                })
                .collect(),
            passwords,
            pooler_mode,
            sharded_tables,
            sharded_schemas,
            multi_tenant: multi_tenant.clone(),
            rw_strategy,
            rw_split,
            schema_admin,
            stats,
            cross_shard_disabled,
            two_phase_commit: two_pc && shards.len() > 1,
            two_phase_commit_auto: two_pc_auto && shards.len() > 1,
            readiness: Arc::new(Readiness::default()),
            rewrite: rewrite.clone(),
            prepared_statements: *prepared_statements,
            dry_run,
            expanded_explain,
            query_parser,
            client_connection_recovery,
            log_min_duration_parse,
            log_query_sample_length,
            reload_schema_on_ddl,
            load_schema,
            resharding_parallel_copies,
            resharding_copy_retry_max_attempts,
            resharding_copy_retry_min_delay: Duration::from_millis(resharding_copy_retry_min_delay),
            resharding_replication_retry_max_attempts,
            resharding_replication_retry_min_delay: Duration::from_millis(
                resharding_replication_retry_min_delay,
            ),
            sharding_lookup_timeout: Duration::from_millis(sharding_lookup_timeout),
            regex_parser: RegexParser::new(regex_parser_limit, query_parser),
            identity: identity.clone(),
            tls_client_certificate_required,
            schema_loader: Box::new(schema_loader::FromServer),
            canonical_oids,
            read_only,
        }
    }

    /// Change config to work with logical replication streaming.
    pub fn logical_stream(&self) -> Self {
        let mut cluster = self.clone();
        // Disable rewrites, we are only sending valid statements.
        cluster.rewrite.enabled = false;
        cluster.rewrite.shard_key = RewriteMode::Ignore;
        cluster.rewrite.split_inserts = RewriteMode::Ignore;
        cluster
    }

    /// Get a connection to a primary of the given shard.
    pub async fn primary(&self, shard: usize, request: &Request) -> Result<Guard, Error> {
        let shard = self.shards.get(shard).ok_or(Error::NoShard(shard))?;
        shard.primary(request).await
    }

    /// Get a connection to a replica of the given shard.
    pub async fn replica(&self, shard: usize, request: &Request) -> Result<Guard, Error> {
        let shard = self.shards.get(shard).ok_or(Error::NoShard(shard))?;
        shard.replica(request).await
    }

    /// The two clusters have the same databases.
    pub(crate) fn can_move_conns_to(&self, other: &Cluster) -> bool {
        self.shards.len() == other.shards.len()
            && self
                .shards
                .iter()
                .zip(other.shards.iter())
                .all(|(a, b)| a.can_move_conns_to(b))
    }

    /// Move connections from cluster to another, saving them.
    pub(crate) fn move_conns_to(&self, other: &Cluster) -> Result<(), Error> {
        for (from, to) in self.shards.iter().zip(other.shards.iter()) {
            from.move_conns_to(to)?;
        }

        Ok(())
    }

    /// Cancel a query executed by one of the shards.
    pub async fn cancel(&self, id: FrontendPid) -> Result<(), super::super::Error> {
        for shard in &self.shards {
            shard.cancel(id).await?;
        }

        Ok(())
    }

    /// Get all shards.
    pub(crate) fn shards(&self) -> &[Shard] {
        &self.shards
    }

    pub fn passwords(&self) -> &[PasswordKind] {
        &self.passwords
    }

    /// Get user identity which should match the TLS certificate it provided
    /// when connecting.
    pub fn identity(&self) -> Option<&str> {
        self.identity.as_deref()
    }

    /// This user must present a client TLS certificate when connecting over TLS.
    pub fn tls_client_certificate_required(&self) -> bool {
        self.tls_client_certificate_required
    }

    /// User name.
    pub fn user(&self) -> &str {
        &self.identifier.user
    }

    /// Cluster name (database name).
    pub fn name(&self) -> &str {
        &self.identifier.database
    }

    /// Get unique cluster identifier.
    pub fn identifier(&self) -> Arc<DatabaseUser> {
        self.identifier.clone()
    }

    /// Get pooler mode.
    pub fn pooler_mode(&self) -> PoolerMode {
        self.pooler_mode
    }

    // Get sharded tables if any.
    pub fn sharded_tables(&self) -> &[ShardedTable] {
        self.sharded_tables.tables()
    }

    /// Get query rewrite config.
    pub fn rewrite(&self) -> &Rewrite {
        &self.rewrite
    }

    pub fn query_parser(&self) -> QueryParserLevel {
        self.query_parser
    }

    pub fn prepared_statements(&self) -> &PreparedStatements {
        &self.prepared_statements
    }

    pub fn client_connection_recovery(&self) -> &ConnectionRecovery {
        &self.client_connection_recovery
    }

    pub fn dry_run(&self) -> bool {
        self.dry_run
    }

    pub fn expanded_explain(&self) -> bool {
        self.expanded_explain
    }

    /// A cluster is read_only if zero shards have a primary,
    /// or if `read_only` for the corresponding `User` is set to true.
    pub fn read_only(&self) -> bool {
        if self.read_only {
            return true;
        }

        for shard in &self.shards {
            if shard.has_primary() {
                return false;
            }
        }

        true
    }

    /// This cluster is write_only if zero shards have a replica.
    pub fn write_only(&self) -> bool {
        for shard in &self.shards {
            if shard.has_replicas() {
                return false;
            }
        }

        true
    }

    /// This database/user pair is responsible for schema management.
    pub fn schema_admin(&self) -> bool {
        self.schema_admin
    }

    /// Change schema owner attribute.
    pub fn toggle_schema_admin(&mut self, owner: bool) {
        self.schema_admin = owner;
    }

    pub fn stats(&self) -> Arc<Mutex<ClusterMetrics>> {
        self.stats.clone()
    }

    /// We'll need the query router to figure out
    /// where a query should go.
    pub fn router_needed(&self) -> bool {
        !(self.shards().len() == 1 && (self.read_only() || self.write_only()))
    }

    /// Use the query parser.
    pub(crate) fn use_query_parser(&self, request: &ClientRequest) -> bool {
        match self.query_parser() {
            QueryParserLevel::Off => false,
            QueryParserLevel::On => true,
            QueryParserLevel::SessionControl | QueryParserLevel::SessionControlAndLocks => {
                self.regex_parser.use_parser(request)
            }
            QueryParserLevel::Auto => {
                self.multi_tenant().is_some()
                    || self.router_needed()
                    || self.dry_run()
                    || self.prepared_statements() == &PreparedStatements::Full
                    || self.regex_parser.use_parser(request)
            }
        }
    }

    /// Multi-tenant config.
    pub fn multi_tenant(&self) -> &Option<MultiTenant> {
        &self.multi_tenant
    }

    /// Get all data required for sharding.
    pub fn sharding_schema(&self) -> ShardingSchema {
        ShardingSchema {
            shards: self.shards.len(),
            tables: self.sharded_tables.clone(),
            schemas: self.sharded_schemas.clone(),
            rewrite: self.rewrite.clone(),
            log_min_duration_parse: self.log_min_duration_parse,
            log_query_sample_length: self.log_query_sample_length,
        }
    }

    pub fn reload_schema(&self) -> bool {
        self.reload_schema_on_ddl && self.load_schema()
    }

    pub(super) fn load_schema(&self) -> bool {
        match self.load_schema {
            LoadSchema::On => true,
            LoadSchema::Off => false,
            LoadSchema::Auto => self.shards.len() > 1 || self.multi_tenant().is_some(),
        }
    }

    /// Get currently loaded schema from shard 0.
    pub fn schema(&self) -> Schema {
        self.shards
            .first()
            .map(|shard| shard.schema())
            .unwrap_or_default()
    }

    /// Read/write strategy
    pub fn read_write_strategy(&self) -> &ReadWriteStrategy {
        &self.rw_strategy
    }

    /// Route queries to the primary by default unless an explicit role hint says otherwise.
    pub(crate) fn prefer_primary(&self) -> bool {
        self.rw_split == ReadWriteSplit::PreferPrimary
    }

    /// Route qeuries to the replicas by default unless an explicit role hint says otherwise.
    pub(crate) fn prefer_replica(&self) -> bool {
        self.rw_split == ReadWriteSplit::ExcludePrimary
    }

    /// Cross-shard queries disabled for this cluster.
    pub fn cross_shard_disabled(&self) -> bool {
        self.cross_shard_disabled
    }

    /// Two-phase commit enabled.
    pub fn two_pc_enabled(&self) -> bool {
        self.two_phase_commit
    }

    /// Two-phase commit transactions started automatically
    /// for single-statement cross-shard writes.
    pub fn two_pc_auto_enabled(&self) -> bool {
        self.two_phase_commit_auto && self.two_pc_enabled()
    }

    /// How many parallel COPY commands can we
    /// run to re-shard this cluster.
    pub fn resharding_parallel_copies(&self) -> usize {
        self.resharding_parallel_copies
    }

    /// Maximum retries for a per-table copy during resharding.
    pub fn resharding_copy_retry_max_attempts(&self) -> usize {
        self.resharding_copy_retry_max_attempts
    }

    /// How long a sharding key lookup query can run before the
    /// statement waiting on it fails.
    pub fn sharding_lookup_timeout(&self) -> Duration {
        self.sharding_lookup_timeout
    }

    /// Base delay between table copy retry attempts. Doubles each attempt, capped at 32×.
    pub fn resharding_copy_retry_min_delay(&self) -> &Duration {
        &self.resharding_copy_retry_min_delay
    }

    /// Maximum consecutive replication-subscriber errors before the error is propagated.
    /// `0` means retry indefinitely.
    pub fn resharding_replication_retry_max_attempts(&self) -> usize {
        self.resharding_replication_retry_max_attempts
    }

    /// Base delay between replication-subscriber retry attempts.
    pub fn resharding_replication_retry_min_delay(&self) -> Duration {
        self.resharding_replication_retry_min_delay
    }

    /// Send a cancellation request for all running queries.
    pub(crate) async fn cancel_all(&self) -> Result<(), Error> {
        let pools: Vec<_> = self
            .shards()
            .iter()
            .flat_map(|shard| shard.pools())
            .collect();

        try_join_all(pools.iter().map(|pool| pool.cancel_all()))
            .await
            .map_err(|_| Error::FastShutdown)?;

        Ok(())
    }

    /// Run a parameterized query on one shard, picked round-robin, and
    /// return all rows. The answer is only authoritative if every shard
    /// has the same data, e.g. an omnisharded table.
    pub async fn fetch_all_round_robin<T: From<DataRow>>(
        &self,
        query: &str,
        params: &[BindParameter],
    ) -> Result<Vec<T>, crate::backend::Error> {
        let shard = self
            .shards
            .get(round_robin::next(self.shards.len().max(1)))
            .ok_or(crate::backend::pool::Error::NoDatabases)?;
        let mut server = shard.primary_or_replica(&Request::default()).await?;

        server
            .fetch_all(ServerRequest::parameterized(query, params))
            .await
    }

    pub(crate) fn is_canonicalizing_oids(&self) -> bool {
        self.canonical_oids.is_some()
    }
}

#[cfg(test)]
mod test {
    use parking_lot::Mutex;
    use std::{sync::Arc, time::Duration};

    use super::ClusterMetrics;
    use pgdog_config::{
        ConfigAndUsers, OmnishardedTable, PoolerMode, QueryParserLevel, ShardedSchema,
    };

    use crate::backend::pool::cluster::SchemaLoader;
    use crate::frontend::router::sharding::ShardedTable;
    use crate::{
        backend::{
            Shard, ShardedTables,
            pool::{Address, Config, PoolConfig, ShardConfig},
            replication::ShardedSchemas,
        },
        config::{DataType, Hasher, MultiTenant, ReadWriteSplit, ReadWriteStrategy, Role, config},
        frontend::ClientRequest,
        net::Query,
    };

    use super::{Cluster, DatabaseUser};

    impl Cluster {
        pub fn new_test(config: &ConfigAndUsers) -> Self {
            let identifier = Arc::new(DatabaseUser {
                user: "pgdog".into(),
                database: "pgdog".into(),
            });
            let primary = Some(&PoolConfig {
                address: Address::default(),
                config: Config::default(),
            });
            let replicas = &[PoolConfig {
                address: Address {
                    configured_role: Role::Replica,
                    ..Default::default()
                },
                config: Config::default(),
            }];

            let shards = (0..2)
                .map(|number| {
                    Shard::new(ShardConfig {
                        number,
                        primary,
                        replicas,
                        identifier: identifier.clone(),
                        lsn_check_interval: Duration::MAX,
                        ..Default::default()
                    })
                })
                .collect::<Vec<_>>();

            let sharded_tables = ShardedTables::new(
                vec![
                    ShardedTable {
                        database: "pgdog".into(),
                        name: Some("sharded".into()),
                        column: "id".into(),
                        primary: true,
                        centroids: vec![],
                        data_type: DataType::Bigint,
                        centroid_probes: 1,
                        hasher: Hasher::Postgres,
                        ..Default::default()
                    },
                    ShardedTable {
                        database: "pgdog".into(),
                        name: Some("posts".into()),
                        column: "id".into(),
                        primary: true,
                        centroids: vec![],
                        data_type: DataType::Bigint,
                        centroid_probes: 1,
                        hasher: Hasher::Postgres,
                        ..Default::default()
                    },
                    // Duplicate-row table for FULL identity ctid-targeting tests.
                    // No primary key on destination — allows identical rows.
                    ShardedTable {
                        database: "pgdog".into(),
                        name: Some("full_dup_rows".into()),
                        column: "id".into(),
                        primary: true,
                        centroids: vec![],
                        data_type: DataType::Bigint,
                        centroid_probes: 1,
                        hasher: Hasher::Postgres,
                        ..Default::default()
                    },
                ],
                vec![
                    OmnishardedTable {
                        name: "sharded_omni".into(),
                        sticky_routing: false,
                    },
                    OmnishardedTable {
                        name: "sharded_omni_sticky".into(),
                        sticky_routing: true,
                    },
                ],
                config.config.general.omnisharded_sticky,
                config.config.general.system_catalogs,
            );
            let stats = Arc::new(Mutex::new(ClusterMetrics {
                lookup: sharded_tables.lookup_cache().stats().clone(),
                ..Default::default()
            }));

            Cluster {
                sharded_tables,
                stats,
                sharded_schemas: ShardedSchemas::new(vec![
                    ShardedSchema {
                        database: "pgdog".into(),
                        name: Some("shard_0".into()),
                        shard: 0,
                        ..Default::default()
                    },
                    ShardedSchema {
                        database: "pgdog".into(),
                        name: Some("shard_1".into()),
                        shard: 1,
                        ..Default::default()
                    },
                ]),
                shards,
                identifier,
                prepared_statements: config.config.general.prepared_statements,
                dry_run: config.config.general.dry_run,
                expanded_explain: config.config.general.expanded_explain,
                query_parser: config.config.general.query_parser,
                regex_parser: crate::frontend::RegexParser::new(
                    config.config.general.regex_parser_limit,
                    config.config.general.query_parser,
                ),
                rewrite: config.config.rewrite.clone(),
                two_phase_commit: config.config.general.two_phase_commit,
                sharding_lookup_timeout: Duration::from_millis(
                    config.config.general.sharding_lookup_timeout,
                ),
                two_phase_commit_auto: config.config.general.two_phase_commit_auto.unwrap_or(false),
                canonical_oids: config
                    .config
                    .general
                    .canonicalize_type_information
                    .then(Default::default),
                ..Default::default()
            }
        }

        pub fn new_test_single_shard(config: &ConfigAndUsers) -> Cluster {
            let mut cluster = Self::new_test(config);
            cluster.shards.pop();

            cluster
        }

        pub fn new_test_session_mode(config: &ConfigAndUsers) -> Cluster {
            let mut cluster = Self::new_test(config);
            cluster.pooler_mode = PoolerMode::Session;
            cluster
        }

        /// Two shards targeting different databases on the same server.
        /// Gives separate lock namespaces without needing two Postgres instances.
        pub fn new_test_two_databases(config: &ConfigAndUsers) -> Cluster {
            let mut cluster = Self::new_test(config);
            let shard1 = cluster.shards.last_mut().unwrap();
            *shard1 = Shard::new(ShardConfig {
                number: 1,
                primary: Some(&PoolConfig {
                    address: Address {
                        database_name: "pgdog1".into(),
                        ..Default::default()
                    },
                    config: Config::default(),
                }),
                replicas: &[PoolConfig {
                    address: Address {
                        database_name: "pgdog1".into(),
                        configured_role: Role::Replica,
                        ..Default::default()
                    },
                    config: Config::default(),
                }],
                identifier: cluster.identifier.clone(),
                lsn_check_interval: Duration::MAX,
                ..Default::default()
            });
            cluster
        }

        pub fn new_test_single_primary(config: &ConfigAndUsers) -> Cluster {
            let identifier = Arc::new(DatabaseUser {
                user: "pgdog".into(),
                database: "pgdog".into(),
            });

            Cluster {
                shards: vec![Shard::new(ShardConfig {
                    primary: Some(&PoolConfig {
                        address: Address::default(),
                        config: Config::default(),
                    }),
                    identifier: identifier.clone(),
                    ..Default::default()
                })],
                prepared_statements: config.config.general.prepared_statements,
                dry_run: config.config.general.dry_run,
                expanded_explain: config.config.general.expanded_explain,
                query_parser: config.config.general.query_parser,
                regex_parser: crate::frontend::RegexParser::new(
                    config.config.general.regex_parser_limit,
                    config.config.general.query_parser,
                ),
                rewrite: config.config.rewrite.clone(),
                two_phase_commit: config.config.general.two_phase_commit,
                sharding_lookup_timeout: Duration::from_millis(
                    config.config.general.sharding_lookup_timeout,
                ),
                two_phase_commit_auto: config.config.general.two_phase_commit_auto.unwrap_or(false),
                ..Default::default()
            }
        }

        pub fn new_test_single_replica(config: &ConfigAndUsers) -> Cluster {
            let mut cluster = Self::new_test_single_shard(config);
            let identifier = cluster.identifier.clone();
            cluster.shards[0] = Shard::new(ShardConfig {
                replicas: &[PoolConfig {
                    address: Address {
                        configured_role: Role::Replica,
                        ..Default::default()
                    },
                    config: Config::default(),
                }],
                identifier,
                ..Default::default()
            });

            cluster
        }

        pub(crate) fn set_read_write_strategy(&mut self, rw_strategy: ReadWriteStrategy) {
            self.rw_strategy = rw_strategy;
        }

        pub(crate) fn set_sharded_tables(&mut self, sharded_tables: ShardedTables) {
            self.stats.lock().lookup = sharded_tables.lookup_cache().stats().clone();
            self.sharded_tables = sharded_tables;
        }

        pub(crate) fn set_sharded_schemas(&mut self, sharded_schemas: ShardedSchemas) {
            self.sharded_schemas = sharded_schemas;
        }

        pub(crate) fn set_rw_split(&mut self, rw_split: ReadWriteSplit) {
            self.rw_split = rw_split;
        }
    }

    #[test]
    fn test_load_schema_multiple_shards_empty_schemas_with_tables() {
        let config = ConfigAndUsers::default();
        let mut cluster = Cluster::new_test(&config);
        cluster.sharded_schemas = ShardedSchemas::default();

        assert!(cluster.load_schema());
    }

    #[test]
    fn test_load_schema_multiple_shards_with_schemas() {
        let config = ConfigAndUsers::default();
        let cluster = Cluster::new_test(&config);

        // In Auto mode with multiple shards, load_schema returns true
        assert!(cluster.load_schema());
    }

    #[test]
    fn test_load_schema_multiple_shards_empty_tables() {
        let config = ConfigAndUsers::default();
        let mut cluster = Cluster::new_test(&config);
        cluster.sharded_schemas = ShardedSchemas::default();
        cluster.sharded_tables = ShardedTables::default();

        // In Auto mode with multiple shards, load_schema returns true
        // (sharded_schemas and sharded_tables no longer affect this decision)
        assert!(cluster.load_schema());
    }

    #[test]
    fn test_load_schema_single_shard() {
        let config = ConfigAndUsers::default();
        let mut cluster = Cluster::new_test_single_shard(&config);
        cluster.sharded_schemas = ShardedSchemas::default();

        assert!(!cluster.load_schema());
    }

    #[test]
    fn test_load_schema_with_multi_tenant() {
        let config = ConfigAndUsers::default();
        let mut cluster = Cluster::new_test_single_shard(&config);
        cluster.multi_tenant = Some(MultiTenant {
            column: "tenant_id".into(),
        });

        assert!(cluster.load_schema());
    }

    #[test]
    fn test_load_schema_multi_tenant_overrides_other_conditions() {
        let config = ConfigAndUsers::default();
        let mut cluster = Cluster::new_test(&config);
        cluster.sharded_tables = ShardedTables::default();
        cluster.multi_tenant = Some(MultiTenant {
            column: "tenant_id".into(),
        });

        assert!(cluster.load_schema());
    }

    #[tokio::test]
    async fn test_launch_sets_online() {
        let config = ConfigAndUsers::default();
        let cluster = Cluster::new_test(&config);

        assert!(!cluster.online());
        cluster.launch();
        assert!(cluster.online());
    }

    #[tokio::test]
    async fn test_shutdown_sets_offline() {
        let config = ConfigAndUsers::default();
        let cluster = Cluster::new_test(&config);

        cluster.launch();
        assert!(cluster.online());
        cluster.shutdown();
        assert!(!cluster.online());
    }

    #[tokio::test]
    async fn test_launch_marks_ready() {
        let config = ConfigAndUsers::default();
        let cluster = Cluster::new_test(&config);

        assert!(!cluster.ready());
        cluster.launch();
        cluster.wait_ready().await;
        assert!(cluster.ready());
    }

    #[tokio::test]
    async fn test_shutdown_releases_readiness_waiters() {
        use tokio::time::{Duration, timeout};

        let config = ConfigAndUsers::default();
        let cluster = Cluster::new_test(&config);

        assert!(!cluster.ready());

        let waiter = cluster.clone();
        let handle = tokio::spawn(async move {
            waiter.wait_ready().await;
        });

        cluster.shutdown();

        let result = timeout(Duration::from_millis(200), handle).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_launch_schema_loading_idempotent() {
        use tokio::time::{Duration, sleep};

        let config = ConfigAndUsers::default();
        let mut cluster = Cluster::new_test(&config);
        cluster.sharded_schemas = ShardedSchemas::default();

        assert!(cluster.load_schema());

        cluster.launch();
        cluster.wait_ready().await;

        // Second launch must be safe: per-shard OnceCell prevents any reload.
        cluster.launch();
        sleep(Duration::from_millis(50)).await;
        cluster.wait_ready().await;
    }

    #[tokio::test]
    async fn test_wait_ready_waits_for_schema_notification() {
        use futures::poll;

        #[derive(Clone)]
        struct ManuallyLoad;

        impl SchemaLoader for ManuallyLoad {
            fn launch_schema_sync(&self, _: &Cluster) {}
        }

        let config = ConfigAndUsers::default();
        let cluster = Cluster {
            schema_loader: Box::new(ManuallyLoad),
            ..Cluster::new_test(&config)
        };

        cluster.launch();

        // Schemas not loaded yet, readiness is pending.
        assert!(!cluster.ready());

        let mut cluster_ready = std::pin::pin!(cluster.wait_ready());

        // Finish loading on all but one shard
        for shard in &cluster.shards[1..] {
            shard.schema_not_needed();
        }

        tokio::task::yield_now().await;
        assert!(poll!(cluster_ready.as_mut()).is_pending());

        cluster.shards[0].schema_not_needed();

        tokio::task::yield_now().await;
        assert!(poll!(cluster_ready.as_mut()).is_ready());
        assert!(cluster.ready());
    }

    #[tokio::test]
    async fn test_wait_ready_returns_immediately_when_schema_not_needed() {
        let config = ConfigAndUsers::default();
        let cluster = Cluster::new_test_single_shard(&config);

        // load_schema() returns false for single shard without multi_tenant
        assert!(!cluster.load_schema());

        cluster.launch();

        // Should return without waiting: no schema to load.
        cluster.wait_ready().await;
        assert!(cluster.ready());
    }

    #[test]
    fn test_use_query_parser_set() {
        let mut cluster = Cluster::new_test(&config());
        let req = ClientRequest::from(vec![Query::new("SET statement_timeout TO 1").into()]);

        for level in [QueryParserLevel::Auto, QueryParserLevel::On] {
            cluster.query_parser = level;
            assert!(cluster.use_query_parser(&req));
        }

        cluster.query_parser = QueryParserLevel::Off;
        assert!(!cluster.use_query_parser(&req));

        let mut cluster = Cluster::new_test_single_primary(&config());

        for level in [QueryParserLevel::Auto, QueryParserLevel::On] {
            cluster.query_parser = level;
            assert!(cluster.use_query_parser(&req));
        }

        cluster.query_parser = QueryParserLevel::Off;
        assert!(!cluster.use_query_parser(&req));
    }
}
