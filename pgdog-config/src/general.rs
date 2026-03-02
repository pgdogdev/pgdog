use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::env;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::Duration;

use crate::pooling::ConnectionRecovery;
use crate::{
    CopyFormat, CutoverTimeoutAction, LoadSchema, QueryParserEngine, QueryParserLevel,
    SystemCatalogsBehavior,
};

use super::auth::{AuthType, PassthoughAuth};
use super::database::{LoadBalancingStrategy, ReadWriteSplit, ReadWriteStrategy};
use super::networking::TlsVerifyMode;
use super::pooling::{PoolerMode, PreparedStatements};

/// General settings are relevant to the operations of the pooler itself, or apply to all database pools.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct General {
    /// The IP address of the local network interface PgDog will bind to listen for connections.
    ///
    /// **Note:** This setting cannot be changed at runtime.
    ///
    /// _Default:_ `0.0.0.0`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#host
    #[serde(default = "General::host")]
    pub host: String,

    /// The TCP port PgDog will bind to listen for connections.
    ///
    /// **Note:** This setting cannot be changed at runtime.
    ///
    /// _Default:_ `6432`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#port
    #[serde(default = "General::port")]
    pub port: u16,

    /// Number of Tokio threads to spawn at pooler startup. In multi-core systems, the recommended setting is two (2) per virtual CPU. The value `0` means to spawn no threads and use the current thread runtime.
    ///
    /// **Note:** This setting cannot be changed at runtime.
    ///
    /// _Default:_ `2`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#workers
    #[serde(default = "General::workers")]
    pub workers: usize,

    /// Default maximum number of server connections per database pool.
    ///
    /// **Note:** We strongly recommend keeping this value well below the supported connections of the backend database(s) to allow connections for maintenance in high load scenarios.
    ///
    /// _Default:_ `10`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#default_pool_size
    #[serde(default = "General::default_pool_size")]
    pub default_pool_size: usize,

    /// Default minimum number of connections per database pool to keep open at all times.
    ///
    /// _Default:_ `1`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#min_pool_size
    #[serde(default = "General::min_pool_size")]
    pub min_pool_size: usize,

    /// Default pooler mode to use for database pools.
    ///
    /// _Default:_ `transaction`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#pooler_mode
    #[serde(default)]
    pub pooler_mode: PoolerMode,

    /// Frequency of healthchecks performed by PgDog to ensure connections provided to clients from the pool are working.
    ///
    /// _Default:_ `30000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#healthcheck_interval
    #[serde(default = "General::healthcheck_interval")]
    pub healthcheck_interval: u64,

    /// Frequency of healthchecks performed by PgDog on idle connections.
    ///
    /// _Default:_ `30000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#idle_healthcheck_interval
    #[serde(default = "General::idle_healthcheck_interval")]
    pub idle_healthcheck_interval: u64,

    /// Delay running idle healthchecks at PgDog startup to give databases (and pools) time to spin up.
    ///
    /// _Default:_ `5000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#idle_healthcheck_delay
    #[serde(default = "General::idle_healthcheck_delay")]
    pub idle_healthcheck_delay: u64,

    /// Maximum amount of time to wait for a healthcheck query to complete.
    ///
    /// _Default:_ `5000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#healthcheck_timeout
    #[serde(default = "General::healthcheck_timeout")]
    pub healthcheck_timeout: u64,

    /// Enable load balancer HTTP health checks with the HTTP server running on this port.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#healthcheck_port
    pub healthcheck_port: Option<u16>,

    /// Connection pools blocked from serving traffic due to an error will be placed back into active rotation after this long.
    ///
    /// _Default:_ `300000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#ban_timeout
    #[serde(default = "General::ban_timeout")]
    pub ban_timeout: u64,

    /// Ban a replica from serving read queries if its replication lag (in milliseconds) exceeds this threshold.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#ban_replica_lag
    #[serde(default = "General::ban_replica_lag")]
    pub ban_replica_lag: u64,

    /// Ban a replica from serving read queries if its replication lag (in bytes) exceeds this threshold.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#ban_replica_lag_bytes
    #[serde(default = "General::ban_replica_lag_bytes")]
    pub ban_replica_lag_bytes: u64,

    /// How long to allow for `ROLLBACK` queries to run on server connections with unfinished transactions.
    ///
    /// _Default:_ `5000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#rollback_timeout
    #[serde(default = "General::rollback_timeout")]
    pub rollback_timeout: u64,

    /// Which strategy to use for load balancing read queries.
    ///
    /// _Default:_ `random`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#load_balancing_strategy
    #[serde(default = "General::load_balancing_strategy")]
    pub load_balancing_strategy: LoadBalancingStrategy,

    /// How aggressive the query parser should be in determining read vs. write queries.
    ///
    /// _Default:_ `conservative`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#read_write_strategy
    #[serde(default)]
    pub read_write_strategy: ReadWriteStrategy,

    /// How to handle the separation of read and write queries.
    ///
    /// _Default:_ `include_primary`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#read_write_split
    #[serde(default)]
    pub read_write_split: ReadWriteSplit,

    /// Path to the TLS certificate PgDog will use to setup TLS connections with clients.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#tls_certificate
    pub tls_certificate: Option<PathBuf>,

    /// Path to the TLS private key PgDog will use to setup TLS connections with clients.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#tls_private_key
    pub tls_private_key: Option<PathBuf>,

    /// Reject clients that connect without TLS.
    ///
    /// _Default:_ `false`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#tls_client_required
    #[serde(default)]
    pub tls_client_required: bool,

    /// How to handle TLS connections to Postgres servers.
    ///
    /// _Default:_ `prefer`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#tls_verify
    #[serde(default = "General::default_tls_verify")]
    pub tls_verify: TlsVerifyMode,

    /// Path to a certificate bundle used to validate the server certificate on TLS connection creation.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#tls_server_ca_certificate
    pub tls_server_ca_certificate: Option<PathBuf>,

    /// How long to wait for active clients to finish transactions when shutting down.
    ///
    /// _Default:_ `60000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#shutdown_timeout
    #[serde(default = "General::default_shutdown_timeout")]
    pub shutdown_timeout: u64,

    /// How long to wait for active connections to be forcibly terminated after `shutdown_timeout` expires.
    ///
    /// **Note:** If set, PgDog will send `CANCEL` requests to PostgreSQL for any remaining active queries before tearing down connection pools.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#shutdown_termination_timeout
    #[serde(default = "General::default_shutdown_termination_timeout")]
    pub shutdown_termination_timeout: Option<u64>,

    /// Broadcast IP address used for multi-instance coordination (e.g., schema cache invalidation across nodes).
    pub broadcast_address: Option<Ipv4Addr>,

    /// UDP port used for multi-instance broadcast coordination.
    #[serde(default = "General::broadcast_port")]
    pub broadcast_port: u16,

    /// Path to a file where all queries are logged. Logging every query is slow; do not use in production.
    #[serde(default)]
    pub query_log: Option<PathBuf>,

    /// The port used for the OpenMetrics HTTP endpoint.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#openmetrics_port
    pub openmetrics_port: Option<u16>,

    /// Prefix added to all metric names exposed via the OpenMetrics endpoint.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#openmetrics_namespace
    pub openmetrics_namespace: Option<String>,

    /// Enables support for prepared statements.
    ///
    /// _Default:_ `extended`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#prepared_statements
    #[serde(default)]
    pub prepared_statements: PreparedStatements,

    /// Deprecated: use [`query_parser`](General::query_parser) set to `"on"` instead.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#query_parser_enabled
    #[serde(default = "General::query_parser_enabled")]
    pub query_parser_enabled: bool,

    /// Toggle the query parser to enable/disable query parsing and all of its benefits. By default, the query parser is turned on automatically, so only disable it if you know what you're doing.
    ///
    /// _Default:_ `auto`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#query_parser
    #[serde(default)]
    pub query_parser: QueryParserLevel,

    /// Underlying parser implementation used to analyze SQL queries.
    #[serde(default)]
    pub query_parser_engine: QueryParserEngine,

    /// Number of prepared statements that will be allowed for each server connection.
    ///
    /// **Note:** If this limit is reached, the least used statement is closed and replaced with the newest one. Additionally, any unused statements in the global cache above this limit will be removed.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#prepared_statements_limit
    #[serde(default = "General::prepared_statements_limit")]
    pub prepared_statements_limit: usize,

    /// Limit on the number of statements saved in the statement cache used to accelerate query parsing.
    ///
    /// _Default:_ `50000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#query_cache_limit
    #[serde(default = "General::query_cache_limit")]
    pub query_cache_limit: usize,

    /// Toggle automatic creation of connection pools given the user name, database and password.
    ///
    /// _Default:_ `disabled`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#passthrough_auth
    #[serde(default = "General::default_passthrough_auth")]
    pub passthrough_auth: PassthoughAuth,

    /// Maximum amount of time to allow for PgDog to create a connection to Postgres.
    ///
    /// _Default:_ `5000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#connect_timeout
    #[serde(default = "General::default_connect_timeout")]
    pub connect_timeout: u64,

    /// Maximum number of retries for Postgres server connection attempts.
    ///
    /// _Default:_ `1`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#connect_attempts
    #[serde(default = "General::connect_attempts")]
    pub connect_attempts: u64,

    /// Amount of time to wait between connection attempt retries.
    ///
    /// _Default:_ `0`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#connect_attempt_delay
    #[serde(default = "General::default_connect_attempt_delay")]
    pub connect_attempt_delay: u64,

    /// Maximum amount of time to wait for a Postgres query to finish executing.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#query_timeout
    #[serde(default = "General::default_query_timeout")]
    pub query_timeout: u64,

    /// Maximum amount of time a client is allowed to wait for a connection from the pool.
    ///
    /// _Default:_ `5000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#checkout_timeout
    #[serde(default = "General::checkout_timeout")]
    pub checkout_timeout: u64,

    /// Maximum amount of time new clients have to complete authentication.
    ///
    /// _Default:_ `60000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#client_login_timeout
    #[serde(default = "General::client_login_timeout")]
    pub client_login_timeout: u64,

    /// Enable the query parser in single-shard deployments and record its decisions.
    ///
    /// _Default:_ `false`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#dry_run
    #[serde(default)]
    pub dry_run: bool,

    /// Close server connections that have been idle, i.e., haven't served a single client transaction, for this amount of time.
    ///
    /// _Default:_ `60000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#idle_timeout
    #[serde(default = "General::idle_timeout")]
    pub idle_timeout: u64,

    /// Close client connections that have been idle, i.e., haven't sent any queries, for this amount of time.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#client_idle_timeout
    #[serde(default = "General::default_client_idle_timeout")]
    pub client_idle_timeout: u64,

    /// Close client connections that have been idle inside a transaction for this amount of time.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#client_idle_in_transaction_timeout
    #[serde(default = "General::default_client_idle_in_transaction_timeout")]
    pub client_idle_in_transaction_timeout: u64,

    /// Maximum amount of time a server connection is allowed to exist.
    ///
    /// _Default:_ `86400000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#server_lifetime
    #[serde(default = "General::server_lifetime")]
    pub server_lifetime: u64,

    /// How many transactions can wait while the mirror database processes previous requests.
    ///
    /// _Default:_ `128`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#mirror_queue
    #[serde(default = "General::mirror_queue")]
    pub mirror_queue: usize,

    /// How many transactions to send to the mirror as a fraction of regular traffic.
    ///
    /// _Default:_ `1.0`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#mirror_exposure
    #[serde(default = "General::mirror_exposure")]
    pub mirror_exposure: f32,

    /// What kind of authentication mechanism to use for client connections.
    ///
    /// _Default:_ `scram`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#auth_type
    #[serde(default)]
    pub auth_type: AuthType,

    /// Disable cross-shard queries globally. When enabled, queries touching more than one shard are rejected.
    #[serde(default)]
    pub cross_shard_disabled: bool,

    /// Overrides the TTL set on DNS records received from DNS servers.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#dns_ttl
    #[serde(default)]
    pub dns_ttl: Option<u64>,

    /// Enables support for pub/sub and configures the size of the background task queue.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#pub_sub_channel_size
    #[serde(default)]
    pub pub_sub_channel_size: usize,

    /// If enabled, log every time a user creates a new connection to PgDog.
    ///
    /// _Default:_ `true`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#log_connections
    #[serde(default = "General::log_connections")]
    pub log_connections: bool,

    /// If enabled, log every time a user disconnects from PgDog.
    ///
    /// _Default:_ `true`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#log_disconnections
    #[serde(default = "General::log_disconnections")]
    pub log_disconnections: bool,

    /// Enable two-phase commit for write, cross-shard transactions.
    ///
    /// _Default:_ `false`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#two_phase_commit
    #[serde(default)]
    pub two_phase_commit: bool,

    /// Enable automatic conversion of single-statement write transactions to use two-phase commit.
    ///
    /// _Default:_ `true`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#two_phase_commit_auto
    #[serde(default)]
    pub two_phase_commit_auto: Option<bool>,

    /// Enable expanded (`\x`) output for `EXPLAIN` results returned by PgDog's built-in query plan aggregation.
    #[serde(default = "General::expanded_explain")]
    pub expanded_explain: bool,

    /// How often to calculate averages shown in `SHOW STATS` admin command and the Prometheus metrics.
    ///
    /// _Default:_ `15000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#stats_period
    #[serde(default = "General::stats_period")]
    pub stats_period: u64,

    /// Controls if server connections are recovered or dropped if a client abruptly disconnects.
    ///
    /// _Default:_ `recover`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#connection_recovery
    #[serde(default = "General::connection_recovery")]
    pub connection_recovery: ConnectionRecovery,

    /// Controls whether to disconnect clients upon encountering connection pool errors.
    ///
    /// **Note:** Set this to `drop` if your clients are async / use pipelining mode.
    ///
    /// _Default:_ `recover`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#client_connection_recovery
    #[serde(default = "General::client_connection_recovery")]
    pub client_connection_recovery: ConnectionRecovery,

    /// How frequently to run the replication delay check.
    ///
    /// _Default:_ `5000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#lsn_check_interval
    #[serde(default = "General::lsn_check_interval")]
    pub lsn_check_interval: u64,

    /// Maximum amount of time allowed for the replication delay query to return a result.
    ///
    /// _Default:_ `5000`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#lsn_check_timeout
    #[serde(default = "General::lsn_check_timeout")]
    pub lsn_check_timeout: u64,

    /// For how long to delay checking for replication delay.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#lsn_check_delay
    #[serde(default = "General::lsn_check_delay")]
    pub lsn_check_delay: u64,

    /// Minimum ID for unique ID generator.
    #[serde(default)]
    pub unique_id_min: u64,

    /// Changes how system catalog tables (like `pg_database`, `pg_class`, etc.) are treated by the query router.
    ///
    /// _Default:_ `omnisharded_sticky`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#system_catalogs
    #[serde(default = "General::default_system_catalogs")]
    pub system_catalogs: SystemCatalogsBehavior,

    /// If turned on, queries touching omnisharded tables are always sent to the same shard for any given client connection. The shard is determined at random on connection creation.
    ///
    /// _Default:_ `false`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#omnisharded_sticky
    #[serde(default)]
    pub omnisharded_sticky: bool,

    /// Which format to use for `COPY` statements during resharding.
    ///
    /// **Note:** Text format is required when migrating from `INTEGER` to `BIGINT` primary keys during resharding.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#resharding_copy_format
    #[serde(default)]
    pub resharding_copy_format: CopyFormat,

    /// Automatically reload the schema cache used by PgDog to route queries upon detecting DDL statements.
    ///
    /// **Note:** This setting requires PgDog Enterprise Edition to work as expected. If using the open source edition, it will only work with single-node PgDog deployments, e.g., in local development or CI.
    ///
    /// _Default:_ `true`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#reload_schema_on_ddl
    #[serde(default = "General::reload_schema_on_ddl")]
    pub reload_schema_on_ddl: bool,

    /// Controls whether PgDog loads the database schema at startup for query routing.
    ///
    /// _Default:_ `auto`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/general/#load_schema
    #[serde(default = "General::load_schema")]
    pub load_schema: LoadSchema,

    /// Maximum number of active transactions allowed before cutover pauses traffic, in milliseconds.
    #[serde(default = "General::cutover_traffic_stop_threshold")]
    pub cutover_traffic_stop_threshold: u64,

    /// Maximum replication lag (in bytes) allowed before proceeding with cutover.
    #[serde(default = "General::cutover_replication_lag_threshold")]
    pub cutover_replication_lag_threshold: u64,

    /// How long to wait after the last transaction drains before completing cutover, in milliseconds.
    #[serde(default = "General::cutover_last_transaction_delay")]
    pub cutover_last_transaction_delay: u64,

    /// How long to wait for cutover conditions to be met before taking the timeout action, in milliseconds.
    #[serde(default = "General::cutover_timeout")]
    pub cutover_timeout: u64,

    /// What to do when the cutover timeout is reached.
    #[serde(default = "General::cutover_timeout_action")]
    pub cutover_timeout_action: CutoverTimeoutAction,

    /// Persist the post-cutover configuration to `pgdog.toml` and `users.toml` on disk.
    #[serde(default)]
    pub cutover_save_config: bool,
}

impl Default for General {
    fn default() -> Self {
        Self {
            host: Self::host(),
            port: Self::port(),
            workers: Self::workers(),
            default_pool_size: Self::default_pool_size(),
            min_pool_size: Self::min_pool_size(),
            pooler_mode: Self::pooler_mode(),
            healthcheck_interval: Self::healthcheck_interval(),
            idle_healthcheck_interval: Self::idle_healthcheck_interval(),
            idle_healthcheck_delay: Self::idle_healthcheck_delay(),
            healthcheck_timeout: Self::healthcheck_timeout(),
            healthcheck_port: Self::healthcheck_port(),
            ban_timeout: Self::ban_timeout(),
            ban_replica_lag: Self::ban_replica_lag(),
            ban_replica_lag_bytes: Self::ban_replica_lag_bytes(),
            rollback_timeout: Self::rollback_timeout(),
            load_balancing_strategy: Self::load_balancing_strategy(),
            read_write_strategy: Self::read_write_strategy(),
            read_write_split: Self::read_write_split(),
            tls_certificate: Self::tls_certificate(),
            tls_private_key: Self::tls_private_key(),
            tls_client_required: bool::default(),
            tls_verify: Self::default_tls_verify(),
            tls_server_ca_certificate: Self::tls_server_ca_certificate(),
            shutdown_timeout: Self::default_shutdown_timeout(),
            shutdown_termination_timeout: Self::default_shutdown_termination_timeout(),
            broadcast_address: Self::broadcast_address(),
            broadcast_port: Self::broadcast_port(),
            query_log: Self::query_log(),
            openmetrics_port: Self::openmetrics_port(),
            openmetrics_namespace: Self::openmetrics_namespace(),
            prepared_statements: Self::prepared_statements(),
            query_parser_enabled: Self::query_parser_enabled(),
            query_parser: QueryParserLevel::default(),
            query_parser_engine: QueryParserEngine::default(),
            prepared_statements_limit: Self::prepared_statements_limit(),
            query_cache_limit: Self::query_cache_limit(),
            passthrough_auth: Self::default_passthrough_auth(),
            connect_timeout: Self::default_connect_timeout(),
            connect_attempt_delay: Self::default_connect_attempt_delay(),
            connect_attempts: Self::connect_attempts(),
            query_timeout: Self::default_query_timeout(),
            checkout_timeout: Self::checkout_timeout(),
            client_login_timeout: Self::client_login_timeout(),
            dry_run: Self::dry_run(),
            idle_timeout: Self::idle_timeout(),
            client_idle_timeout: Self::default_client_idle_timeout(),
            client_idle_in_transaction_timeout: Self::default_client_idle_in_transaction_timeout(),
            mirror_queue: Self::mirror_queue(),
            mirror_exposure: Self::mirror_exposure(),
            auth_type: Self::auth_type(),
            cross_shard_disabled: Self::cross_shard_disabled(),
            dns_ttl: Self::default_dns_ttl(),
            pub_sub_channel_size: Self::pub_sub_channel_size(),
            log_connections: Self::log_connections(),
            log_disconnections: Self::log_disconnections(),
            two_phase_commit: bool::default(),
            two_phase_commit_auto: None,
            expanded_explain: Self::expanded_explain(),
            server_lifetime: Self::server_lifetime(),
            stats_period: Self::stats_period(),
            connection_recovery: Self::connection_recovery(),
            client_connection_recovery: Self::client_connection_recovery(),
            lsn_check_interval: Self::lsn_check_interval(),
            lsn_check_timeout: Self::lsn_check_timeout(),
            lsn_check_delay: Self::lsn_check_delay(),
            unique_id_min: u64::default(),
            system_catalogs: Self::default_system_catalogs(),
            omnisharded_sticky: bool::default(),
            resharding_copy_format: CopyFormat::default(),
            reload_schema_on_ddl: Self::reload_schema_on_ddl(),
            load_schema: Self::load_schema(),
            cutover_replication_lag_threshold: Self::cutover_replication_lag_threshold(),
            cutover_traffic_stop_threshold: Self::cutover_traffic_stop_threshold(),
            cutover_last_transaction_delay: Self::cutover_last_transaction_delay(),
            cutover_timeout: Self::cutover_timeout(),
            cutover_timeout_action: Self::cutover_timeout_action(),
            cutover_save_config: bool::default(),
        }
    }
}

impl General {
    fn env_or_default<T: std::str::FromStr>(env_var: &str, default: T) -> T {
        env::var(env_var)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    fn env_string_or_default(env_var: &str, default: &str) -> String {
        env::var(env_var).unwrap_or_else(|_| default.to_string())
    }

    fn env_bool_or_default(env_var: &str, default: bool) -> bool {
        env::var(env_var)
            .ok()
            .and_then(|v| match v.to_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => Some(true),
                "false" | "0" | "no" | "off" => Some(false),
                _ => None,
            })
            .unwrap_or(default)
    }

    fn env_option<T: std::str::FromStr>(env_var: &str) -> Option<T> {
        env::var(env_var).ok().and_then(|v| v.parse().ok())
    }

    fn env_option_string(env_var: &str) -> Option<String> {
        env::var(env_var).ok().filter(|s| !s.is_empty())
    }

    fn env_enum_or_default<T: std::str::FromStr + Default>(env_var: &str) -> T {
        env::var(env_var)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_default()
    }

    fn host() -> String {
        Self::env_string_or_default("PGDOG_HOST", "0.0.0.0")
    }

    pub fn port() -> u16 {
        Self::env_or_default("PGDOG_PORT", 6432)
    }

    fn workers() -> usize {
        Self::env_or_default("PGDOG_WORKERS", 2)
    }

    fn default_pool_size() -> usize {
        Self::env_or_default("PGDOG_DEFAULT_POOL_SIZE", 10)
    }

    fn min_pool_size() -> usize {
        Self::env_or_default("PGDOG_MIN_POOL_SIZE", 1)
    }

    fn healthcheck_interval() -> u64 {
        Self::env_or_default("PGDOG_HEALTHCHECK_INTERVAL", 30_000)
    }

    fn reload_schema_on_ddl() -> bool {
        Self::env_bool_or_default("PGDOG_SCHEMA_RELOAD_ON_DDL", true)
    }

    fn idle_healthcheck_interval() -> u64 {
        Self::env_or_default("PGDOG_IDLE_HEALTHCHECK_INTERVAL", 30_000)
    }

    fn idle_healthcheck_delay() -> u64 {
        Self::env_or_default("PGDOG_IDLE_HEALTHCHECK_DELAY", 5_000)
    }

    fn healthcheck_port() -> Option<u16> {
        Self::env_option("PGDOG_HEALTHCHECK_PORT")
    }

    fn ban_timeout() -> u64 {
        Self::env_or_default(
            "PGDOG_BAN_TIMEOUT",
            Duration::from_secs(300).as_millis() as u64,
        )
    }

    fn ban_replica_lag() -> u64 {
        // Use i64::MAX to ensure TOML serialization compatibility (TOML only supports i64)
        Self::env_or_default("PGDOG_BAN_REPLICA_LAG", i64::MAX as u64)
    }

    fn ban_replica_lag_bytes() -> u64 {
        // Use i64::MAX to ensure TOML serialization compatibility (TOML only supports i64)
        Self::env_or_default("PGDOG_BAN_REPLICA_LAG_BYTES", i64::MAX as u64)
    }

    fn cutover_replication_lag_threshold() -> u64 {
        Self::env_or_default("PGDOG_CUTOVER_REPLICATION_LAG_THRESHOLD", 1_000)
        // 1KB
    }

    fn cutover_traffic_stop_threshold() -> u64 {
        Self::env_or_default("PGDOG_CUTOVER_TRAFFIC_STOP_THRESHOLD", 1_000_000)
        // 1MB
    }

    fn cutover_last_transaction_delay() -> u64 {
        Self::env_or_default("PGDOG_CUTOVER_LAST_TRANSACTION_DELAY", 1_000) // 1 second
    }

    fn cutover_timeout() -> u64 {
        Self::env_or_default("PGDOG_CUTOVER_TIMEOUT", 30_000)
        // 30 seconds
    }

    fn cutover_timeout_action() -> CutoverTimeoutAction {
        Self::env_enum_or_default("PGDOG_CUTOVER_TIMEOUT_ACTION")
    }

    fn rollback_timeout() -> u64 {
        Self::env_or_default("PGDOG_ROLLBACK_TIMEOUT", 5_000)
    }

    fn idle_timeout() -> u64 {
        Self::env_or_default(
            "PGDOG_IDLE_TIMEOUT",
            Duration::from_secs(60).as_millis() as u64,
        )
    }

    fn client_login_timeout() -> u64 {
        Self::env_or_default(
            "PGDOG_CLIENT_LOG_TIMEOUT",
            Duration::from_secs(60).as_millis() as u64,
        )
    }

    fn default_client_idle_timeout() -> u64 {
        Self::env_or_default(
            "PGDOG_CLIENT_IDLE_TIMEOUT",
            crate::MAX_DURATION.as_millis() as u64,
        )
    }

    fn default_client_idle_in_transaction_timeout() -> u64 {
        Self::env_or_default(
            "PGDOG_CLIENT_IDLE_IN_TRANSACTION_TIMEOUT",
            crate::MAX_DURATION.as_millis() as u64,
        )
    }

    fn default_query_timeout() -> u64 {
        Self::env_or_default(
            "PGDOG_QUERY_TIMEOUT",
            crate::MAX_DURATION.as_millis() as u64,
        )
    }

    pub fn query_timeout(&self) -> Duration {
        Duration::from_millis(self.query_timeout)
    }

    pub fn dns_ttl(&self) -> Option<Duration> {
        self.dns_ttl.map(Duration::from_millis)
    }

    pub fn client_idle_timeout(&self) -> Duration {
        Duration::from_millis(self.client_idle_timeout)
    }

    pub fn connect_attempt_delay(&self) -> Duration {
        Duration::from_millis(self.connect_attempt_delay)
    }

    pub fn client_idle_in_transaction_timeout(&self) -> Duration {
        Duration::from_millis(self.client_idle_in_transaction_timeout)
    }

    fn load_balancing_strategy() -> LoadBalancingStrategy {
        Self::env_enum_or_default("PGDOG_LOAD_BALANCING_STRATEGY")
    }

    fn default_tls_verify() -> TlsVerifyMode {
        env::var("PGDOG_TLS_VERIFY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(TlsVerifyMode::Prefer)
    }

    fn default_shutdown_timeout() -> u64 {
        Self::env_or_default("PGDOG_SHUTDOWN_TIMEOUT", 60_000)
    }

    fn default_system_catalogs() -> SystemCatalogsBehavior {
        Self::env_enum_or_default("PGDOG_SYSTEM_CATALOGS")
    }

    fn default_shutdown_termination_timeout() -> Option<u64> {
        Self::env_option("PGDOG_SHUTDOWN_TERMINATION_TIMEOUT")
    }

    fn default_connect_timeout() -> u64 {
        Self::env_or_default("PGDOG_CONNECT_TIMEOUT", 5_000)
    }

    fn default_connect_attempt_delay() -> u64 {
        Self::env_or_default("PGDOG_CONNECT_ATTEMPT_DELAY", 0)
    }

    fn connect_attempts() -> u64 {
        Self::env_or_default("PGDOG_CONNECT_ATTEMPTS", 1)
    }

    fn pooler_mode() -> PoolerMode {
        Self::env_enum_or_default("PGDOG_POOLER_MODE")
    }

    fn lsn_check_timeout() -> u64 {
        Self::env_or_default("PGDOG_LSN_CHECK_TIMEOUT", 5_000)
    }

    fn lsn_check_interval() -> u64 {
        Self::env_or_default("PGDOG_LSN_CHECK_INTERVAL", 5_000)
    }

    fn lsn_check_delay() -> u64 {
        Self::env_or_default(
            "PGDOG_LSN_CHECK_DELAY",
            crate::MAX_DURATION.as_millis() as u64,
        )
    }

    fn read_write_strategy() -> ReadWriteStrategy {
        Self::env_enum_or_default("PGDOG_READ_WRITE_STRATEGY")
    }

    fn read_write_split() -> ReadWriteSplit {
        Self::env_enum_or_default("PGDOG_READ_WRITE_SPLIT")
    }

    fn prepared_statements() -> PreparedStatements {
        Self::env_enum_or_default("PGDOG_PREPARED_STATEMENTS")
    }

    fn query_parser_enabled() -> bool {
        Self::env_bool_or_default("PGDOG_QUERY_PARSER_ENABLED", false)
    }

    fn auth_type() -> AuthType {
        Self::env_enum_or_default("PGDOG_AUTH_TYPE")
    }

    fn tls_certificate() -> Option<PathBuf> {
        Self::env_option_string("PGDOG_TLS_CERTIFICATE").map(PathBuf::from)
    }

    fn tls_private_key() -> Option<PathBuf> {
        Self::env_option_string("PGDOG_TLS_PRIVATE_KEY").map(PathBuf::from)
    }

    fn tls_server_ca_certificate() -> Option<PathBuf> {
        Self::env_option_string("PGDOG_TLS_SERVER_CA_CERTIFICATE").map(PathBuf::from)
    }

    fn query_log() -> Option<PathBuf> {
        Self::env_option_string("PGDOG_QUERY_LOG").map(PathBuf::from)
    }

    pub fn openmetrics_port() -> Option<u16> {
        Self::env_option("PGDOG_OPENMETRICS_PORT")
    }

    pub fn openmetrics_namespace() -> Option<String> {
        Self::env_option_string("PGDOG_OPENMETRICS_NAMESPACE")
    }

    fn default_dns_ttl() -> Option<u64> {
        Self::env_option("PGDOG_DNS_TTL")
    }

    pub fn pub_sub_channel_size() -> usize {
        Self::env_or_default("PGDOG_PUB_SUB_CHANNEL_SIZE", 0)
    }

    pub fn dry_run() -> bool {
        Self::env_bool_or_default("PGDOG_DRY_RUN", false)
    }

    pub fn cross_shard_disabled() -> bool {
        Self::env_bool_or_default("PGDOG_CROSS_SHARD_DISABLED", false)
    }

    pub fn broadcast_address() -> Option<Ipv4Addr> {
        Self::env_option("PGDOG_BROADCAST_ADDRESS")
    }

    pub fn broadcast_port() -> u16 {
        Self::env_or_default("PGDOG_BROADCAST_PORT", Self::port() + 1)
    }

    fn healthcheck_timeout() -> u64 {
        Self::env_or_default(
            "PGDOG_HEALTHCHECK_TIMEOUT",
            Duration::from_secs(5).as_millis() as u64,
        )
    }

    fn checkout_timeout() -> u64 {
        Self::env_or_default(
            "PGDOG_CHECKOUT_TIMEOUT",
            Duration::from_secs(5).as_millis() as u64,
        )
    }

    fn load_schema() -> LoadSchema {
        Self::env_enum_or_default("PGDOG_LOAD_SCHEMA")
    }

    pub fn mirror_queue() -> usize {
        Self::env_or_default("PGDOG_MIRROR_QUEUE", 128)
    }

    pub fn mirror_exposure() -> f32 {
        Self::env_or_default("PGDOG_MIRROR_EXPOSURE", 1.0)
    }

    pub fn prepared_statements_limit() -> usize {
        Self::env_or_default("PGDOG_PREPARED_STATEMENTS_LIMIT", i64::MAX as usize)
    }

    pub fn query_cache_limit() -> usize {
        Self::env_or_default("PGDOG_QUERY_CACHE_LIMIT", 50_000)
    }

    pub fn log_connections() -> bool {
        Self::env_bool_or_default("PGDOG_LOG_CONNECTIONS", true)
    }

    pub fn log_disconnections() -> bool {
        Self::env_bool_or_default("PGDOG_LOG_DISCONNECTIONS", true)
    }

    pub fn expanded_explain() -> bool {
        Self::env_bool_or_default("PGDOG_EXPANDED_EXPLAIN", false)
    }

    pub fn server_lifetime() -> u64 {
        Self::env_or_default(
            "PGDOG_SERVER_LIFETIME",
            Duration::from_secs(3600 * 24).as_millis() as u64,
        )
    }

    pub fn connection_recovery() -> ConnectionRecovery {
        Self::env_enum_or_default("PGDOG_CONNECTION_RECOVERY")
    }

    pub fn client_connection_recovery() -> ConnectionRecovery {
        Self::env_option("PGDOG_CLIENT_CONNECTION_RECOVERY").unwrap_or(ConnectionRecovery::Drop)
    }

    fn stats_period() -> u64 {
        Self::env_or_default("PGDOG_STATS_PERIOD", 15_000)
    }

    fn default_passthrough_auth() -> PassthoughAuth {
        if let Ok(auth) = env::var("PGDOG_PASSTHROUGH_AUTH") {
            // TODO: figure out why toml::from_str doesn't work.
            match auth.as_str() {
                "enabled" => PassthoughAuth::Enabled,
                "disabled" => PassthoughAuth::Disabled,
                "enabled_plain" => PassthoughAuth::EnabledPlain,
                _ => PassthoughAuth::default(),
            }
        } else {
            PassthoughAuth::default()
        }
    }

    /// Get shutdown timeout as a duration.
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_millis(self.shutdown_timeout)
    }

    pub fn shutdown_termination_timeout(&self) -> Option<Duration> {
        self.shutdown_termination_timeout.map(Duration::from_millis)
    }

    /// Get TLS config, if any.
    pub fn tls(&self) -> Option<(&PathBuf, &PathBuf)> {
        if let Some(cert) = &self.tls_certificate {
            if let Some(key) = &self.tls_private_key {
                return Some((cert, key));
            }
        }

        None
    }

    pub fn passthrough_auth(&self) -> bool {
        self.tls().is_some() && self.passthrough_auth == PassthoughAuth::Enabled
            || self.passthrough_auth == PassthoughAuth::EnabledPlain
    }

    /// Support for LISTEN/NOTIFY.
    pub fn pub_sub_enabled(&self) -> bool {
        self.pub_sub_channel_size > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_env_workers() {
        env::set_var("PGDOG_WORKERS", "8");
        assert_eq!(General::workers(), 8);
        env::remove_var("PGDOG_WORKERS");
        assert_eq!(General::workers(), 2);
    }

    #[test]
    fn test_env_pool_sizes() {
        env::set_var("PGDOG_DEFAULT_POOL_SIZE", "50");
        env::set_var("PGDOG_MIN_POOL_SIZE", "5");

        assert_eq!(General::default_pool_size(), 50);
        assert_eq!(General::min_pool_size(), 5);

        env::remove_var("PGDOG_DEFAULT_POOL_SIZE");
        env::remove_var("PGDOG_MIN_POOL_SIZE");

        assert_eq!(General::default_pool_size(), 10);
        assert_eq!(General::min_pool_size(), 1);
    }

    #[test]
    fn test_env_timeouts() {
        env::set_var("PGDOG_HEALTHCHECK_INTERVAL", "60000");
        env::set_var("PGDOG_HEALTHCHECK_TIMEOUT", "10000");
        env::set_var("PGDOG_CONNECT_TIMEOUT", "10000");
        env::set_var("PGDOG_CHECKOUT_TIMEOUT", "15000");
        env::set_var("PGDOG_IDLE_TIMEOUT", "120000");

        assert_eq!(General::healthcheck_interval(), 60000);
        assert_eq!(General::healthcheck_timeout(), 10000);
        assert_eq!(General::default_connect_timeout(), 10000);
        assert_eq!(General::checkout_timeout(), 15000);
        assert_eq!(General::idle_timeout(), 120000);

        env::remove_var("PGDOG_HEALTHCHECK_INTERVAL");
        env::remove_var("PGDOG_HEALTHCHECK_TIMEOUT");
        env::remove_var("PGDOG_CONNECT_TIMEOUT");
        env::remove_var("PGDOG_CHECKOUT_TIMEOUT");
        env::remove_var("PGDOG_IDLE_TIMEOUT");

        assert_eq!(General::healthcheck_interval(), 30000);
        assert_eq!(General::healthcheck_timeout(), 5000);
        assert_eq!(General::default_connect_timeout(), 5000);
        assert_eq!(General::checkout_timeout(), 5000);
        assert_eq!(General::idle_timeout(), 60000);
    }

    #[test]
    fn test_env_invalid_values() {
        env::set_var("PGDOG_WORKERS", "invalid");
        env::set_var("PGDOG_DEFAULT_POOL_SIZE", "not_a_number");

        assert_eq!(General::workers(), 2);
        assert_eq!(General::default_pool_size(), 10);

        env::remove_var("PGDOG_WORKERS");
        env::remove_var("PGDOG_DEFAULT_POOL_SIZE");
    }

    #[test]
    fn test_env_host_port() {
        // Test existing env var functionality
        env::set_var("PGDOG_HOST", "192.168.1.1");
        env::set_var("PGDOG_PORT", "8432");

        assert_eq!(General::host(), "192.168.1.1");
        assert_eq!(General::port(), 8432);

        env::remove_var("PGDOG_HOST");
        env::remove_var("PGDOG_PORT");

        assert_eq!(General::host(), "0.0.0.0");
        assert_eq!(General::port(), 6432);
    }

    #[test]
    fn test_env_enum_fields() {
        // Test pooler mode
        env::set_var("PGDOG_POOLER_MODE", "session");
        assert_eq!(General::pooler_mode(), PoolerMode::Session);
        env::remove_var("PGDOG_POOLER_MODE");
        assert_eq!(General::pooler_mode(), PoolerMode::Transaction);

        // Test load balancing strategy
        env::set_var("PGDOG_LOAD_BALANCING_STRATEGY", "round_robin");
        assert_eq!(
            General::load_balancing_strategy(),
            LoadBalancingStrategy::RoundRobin
        );
        env::remove_var("PGDOG_LOAD_BALANCING_STRATEGY");
        assert_eq!(
            General::load_balancing_strategy(),
            LoadBalancingStrategy::Random
        );

        // Test read-write strategy
        env::set_var("PGDOG_READ_WRITE_STRATEGY", "aggressive");
        assert_eq!(
            General::read_write_strategy(),
            ReadWriteStrategy::Aggressive
        );
        env::remove_var("PGDOG_READ_WRITE_STRATEGY");
        assert_eq!(
            General::read_write_strategy(),
            ReadWriteStrategy::Conservative
        );

        // Test read-write split
        env::set_var("PGDOG_READ_WRITE_SPLIT", "exclude_primary");
        assert_eq!(General::read_write_split(), ReadWriteSplit::ExcludePrimary);
        env::remove_var("PGDOG_READ_WRITE_SPLIT");
        assert_eq!(General::read_write_split(), ReadWriteSplit::IncludePrimary);

        // Test TLS verify mode
        env::set_var("PGDOG_TLS_VERIFY", "verify_full");
        assert_eq!(General::default_tls_verify(), TlsVerifyMode::VerifyFull);
        env::remove_var("PGDOG_TLS_VERIFY");
        assert_eq!(General::default_tls_verify(), TlsVerifyMode::Prefer);

        // Test prepared statements
        env::set_var("PGDOG_PREPARED_STATEMENTS", "full");
        assert_eq!(General::prepared_statements(), PreparedStatements::Full);
        env::remove_var("PGDOG_PREPARED_STATEMENTS");
        assert_eq!(General::prepared_statements(), PreparedStatements::Extended);

        // Test auth type
        env::set_var("PGDOG_AUTH_TYPE", "md5");
        assert_eq!(General::auth_type(), AuthType::Md5);
        env::remove_var("PGDOG_AUTH_TYPE");
        assert_eq!(General::auth_type(), AuthType::Scram);
    }

    #[test]
    fn test_env_additional_timeouts() {
        env::set_var("PGDOG_IDLE_HEALTHCHECK_INTERVAL", "45000");
        env::set_var("PGDOG_IDLE_HEALTHCHECK_DELAY", "10000");
        env::set_var("PGDOG_BAN_TIMEOUT", "600000");
        env::set_var("PGDOG_ROLLBACK_TIMEOUT", "10000");
        env::set_var("PGDOG_SHUTDOWN_TIMEOUT", "120000");
        env::set_var("PGDOG_SHUTDOWN_TERMINATION_TIMEOUT", "15000");
        env::set_var("PGDOG_CONNECT_ATTEMPT_DELAY", "1000");
        env::set_var("PGDOG_QUERY_TIMEOUT", "30000");
        env::set_var("PGDOG_CLIENT_IDLE_TIMEOUT", "3600000");

        assert_eq!(General::idle_healthcheck_interval(), 45000);
        assert_eq!(General::idle_healthcheck_delay(), 10000);
        assert_eq!(General::ban_timeout(), 600000);
        assert_eq!(General::rollback_timeout(), 10000);
        assert_eq!(General::default_shutdown_timeout(), 120000);
        assert_eq!(
            General::default_shutdown_termination_timeout(),
            Some(15_000)
        );
        assert_eq!(General::default_connect_attempt_delay(), 1000);
        assert_eq!(General::default_query_timeout(), 30000);
        assert_eq!(General::default_client_idle_timeout(), 3600000);

        env::remove_var("PGDOG_IDLE_HEALTHCHECK_INTERVAL");
        env::remove_var("PGDOG_IDLE_HEALTHCHECK_DELAY");
        env::remove_var("PGDOG_BAN_TIMEOUT");
        env::remove_var("PGDOG_ROLLBACK_TIMEOUT");
        env::remove_var("PGDOG_SHUTDOWN_TIMEOUT");
        env::remove_var("PGDOG_SHUTDOWN_TERMINATION_TIMEOUT");
        env::remove_var("PGDOG_CONNECT_ATTEMPT_DELAY");
        env::remove_var("PGDOG_QUERY_TIMEOUT");
        env::remove_var("PGDOG_CLIENT_IDLE_TIMEOUT");

        assert_eq!(General::idle_healthcheck_interval(), 30000);
        assert_eq!(General::idle_healthcheck_delay(), 5000);
        assert_eq!(General::ban_timeout(), 300000);
        assert_eq!(General::rollback_timeout(), 5000);
        assert_eq!(General::default_shutdown_timeout(), 60000);
        assert_eq!(General::default_shutdown_termination_timeout(), None);
        assert_eq!(General::default_connect_attempt_delay(), 0);
    }

    #[test]
    fn test_env_path_fields() {
        env::set_var("PGDOG_TLS_CERTIFICATE", "/path/to/cert.pem");
        env::set_var("PGDOG_TLS_PRIVATE_KEY", "/path/to/key.pem");
        env::set_var("PGDOG_TLS_SERVER_CA_CERTIFICATE", "/path/to/ca.pem");
        env::set_var("PGDOG_QUERY_LOG", "/var/log/pgdog/queries.log");

        assert_eq!(
            General::tls_certificate(),
            Some(PathBuf::from("/path/to/cert.pem"))
        );
        assert_eq!(
            General::tls_private_key(),
            Some(PathBuf::from("/path/to/key.pem"))
        );
        assert_eq!(
            General::tls_server_ca_certificate(),
            Some(PathBuf::from("/path/to/ca.pem"))
        );
        assert_eq!(
            General::query_log(),
            Some(PathBuf::from("/var/log/pgdog/queries.log"))
        );

        env::remove_var("PGDOG_TLS_CERTIFICATE");
        env::remove_var("PGDOG_TLS_PRIVATE_KEY");
        env::remove_var("PGDOG_TLS_SERVER_CA_CERTIFICATE");
        env::remove_var("PGDOG_QUERY_LOG");

        assert_eq!(General::tls_certificate(), None);
        assert_eq!(General::tls_private_key(), None);
        assert_eq!(General::tls_server_ca_certificate(), None);
        assert_eq!(General::query_log(), None);
    }

    #[test]
    fn test_env_numeric_fields() {
        env::set_var("PGDOG_BROADCAST_PORT", "7432");
        env::set_var("PGDOG_OPENMETRICS_PORT", "9090");
        env::set_var("PGDOG_PREPARED_STATEMENTS_LIMIT", "1000");
        env::set_var("PGDOG_QUERY_CACHE_LIMIT", "500");
        env::set_var("PGDOG_CONNECT_ATTEMPTS", "3");
        env::set_var("PGDOG_MIRROR_QUEUE", "256");
        env::set_var("PGDOG_MIRROR_EXPOSURE", "0.5");
        env::set_var("PGDOG_DNS_TTL", "60000");
        env::set_var("PGDOG_PUB_SUB_CHANNEL_SIZE", "100");

        assert_eq!(General::broadcast_port(), 7432);
        assert_eq!(General::openmetrics_port(), Some(9090));
        assert_eq!(General::prepared_statements_limit(), 1000);
        assert_eq!(General::query_cache_limit(), 500);
        assert_eq!(General::connect_attempts(), 3);
        assert_eq!(General::mirror_queue(), 256);
        assert_eq!(General::mirror_exposure(), 0.5);
        assert_eq!(General::default_dns_ttl(), Some(60000));
        assert_eq!(General::pub_sub_channel_size(), 100);

        env::remove_var("PGDOG_BROADCAST_PORT");
        env::remove_var("PGDOG_OPENMETRICS_PORT");
        env::remove_var("PGDOG_PREPARED_STATEMENTS_LIMIT");
        env::remove_var("PGDOG_QUERY_CACHE_LIMIT");
        env::remove_var("PGDOG_CONNECT_ATTEMPTS");
        env::remove_var("PGDOG_MIRROR_QUEUE");
        env::remove_var("PGDOG_MIRROR_EXPOSURE");
        env::remove_var("PGDOG_DNS_TTL");
        env::remove_var("PGDOG_PUB_SUB_CHANNEL_SIZE");

        assert_eq!(General::broadcast_port(), General::port() + 1);
        assert_eq!(General::openmetrics_port(), None);
        assert_eq!(General::prepared_statements_limit(), i64::MAX as usize);
        assert_eq!(General::query_cache_limit(), 50_000);
        assert_eq!(General::connect_attempts(), 1);
        assert_eq!(General::mirror_queue(), 128);
        assert_eq!(General::mirror_exposure(), 1.0);
        assert_eq!(General::default_dns_ttl(), None);
        assert_eq!(General::pub_sub_channel_size(), 0);
    }

    #[test]
    fn test_env_boolean_fields() {
        env::set_var("PGDOG_DRY_RUN", "true");
        env::set_var("PGDOG_CROSS_SHARD_DISABLED", "yes");
        env::set_var("PGDOG_LOG_CONNECTIONS", "false");
        env::set_var("PGDOG_LOG_DISCONNECTIONS", "0");

        assert_eq!(General::dry_run(), true);
        assert_eq!(General::cross_shard_disabled(), true);
        assert_eq!(General::log_connections(), false);
        assert_eq!(General::log_disconnections(), false);

        env::remove_var("PGDOG_DRY_RUN");
        env::remove_var("PGDOG_CROSS_SHARD_DISABLED");
        env::remove_var("PGDOG_LOG_CONNECTIONS");
        env::remove_var("PGDOG_LOG_DISCONNECTIONS");

        assert_eq!(General::dry_run(), false);
        assert_eq!(General::cross_shard_disabled(), false);
        assert_eq!(General::log_connections(), true);
        assert_eq!(General::log_disconnections(), true);
    }

    #[test]
    fn test_env_other_fields() {
        env::set_var("PGDOG_BROADCAST_ADDRESS", "192.168.1.100");
        env::set_var("PGDOG_OPENMETRICS_NAMESPACE", "pgdog_metrics");

        assert_eq!(
            General::broadcast_address(),
            Some("192.168.1.100".parse().unwrap())
        );
        assert_eq!(
            General::openmetrics_namespace(),
            Some("pgdog_metrics".to_string())
        );

        env::remove_var("PGDOG_BROADCAST_ADDRESS");
        env::remove_var("PGDOG_OPENMETRICS_NAMESPACE");

        assert_eq!(General::broadcast_address(), None);
        assert_eq!(General::openmetrics_namespace(), None);
    }

    #[test]
    fn test_env_invalid_enum_values() {
        env::set_var("PGDOG_POOLER_MODE", "invalid_mode");
        env::set_var("PGDOG_AUTH_TYPE", "not_an_auth");
        env::set_var("PGDOG_TLS_VERIFY", "bad_verify");

        // Should fall back to defaults for invalid values
        assert_eq!(General::pooler_mode(), PoolerMode::Transaction);
        assert_eq!(General::auth_type(), AuthType::Scram);
        assert_eq!(General::default_tls_verify(), TlsVerifyMode::Prefer);

        env::remove_var("PGDOG_POOLER_MODE");
        env::remove_var("PGDOG_AUTH_TYPE");
        env::remove_var("PGDOG_TLS_VERIFY");
    }

    #[test]
    fn test_general_default_uses_env_vars() {
        // Set some environment variables
        env::set_var("PGDOG_WORKERS", "8");
        env::set_var("PGDOG_POOLER_MODE", "session");
        env::set_var("PGDOG_AUTH_TYPE", "trust");
        env::set_var("PGDOG_DRY_RUN", "true");

        let general = General::default();

        assert_eq!(general.workers, 8);
        assert_eq!(general.pooler_mode, PoolerMode::Session);
        assert_eq!(general.auth_type, AuthType::Trust);
        assert_eq!(general.dry_run, true);

        env::remove_var("PGDOG_WORKERS");
        env::remove_var("PGDOG_POOLER_MODE");
        env::remove_var("PGDOG_AUTH_TYPE");
        env::remove_var("PGDOG_DRY_RUN");
    }
}
