use serde::{Deserialize, Serialize};
use std::env;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::Duration;

use crate::pooling::ConnectionRecovery;

use crate::{
    CopyFormat, CrossShardBackend, LoadSchema, QueryParserEngine, QueryParserLevel,
    SystemCatalogsBehavior,
};

use super::auth::{AuthType, PassthoughAuth};
use super::database::{LoadBalancingStrategy, ReadWriteSplit, ReadWriteStrategy};
use super::networking::TlsVerifyMode;
use super::pooling::{PoolerMode, PreparedStatements};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct General {
    /// Run on this address.
    #[serde(default = "General::host")]
    pub host: String,
    /// Run on this port.
    #[serde(default = "General::port")]
    pub port: u16,
    /// Spawn this many Tokio threads.
    #[serde(default = "General::workers")]
    pub workers: usize,
    /// Default pool size, e.g. 10.
    #[serde(default = "General::default_pool_size")]
    pub default_pool_size: usize,
    /// Minimum number of connections to maintain in the pool.
    #[serde(default = "General::min_pool_size")]
    pub min_pool_size: usize,
    /// Pooler mode, e.g. transaction.
    #[serde(default)]
    pub pooler_mode: PoolerMode,
    /// How often to check a connection.
    #[serde(default = "General::healthcheck_interval")]
    pub healthcheck_interval: u64,
    /// How often to issue a healthcheck via an idle connection.
    #[serde(default = "General::idle_healthcheck_interval")]
    pub idle_healthcheck_interval: u64,
    /// Delay idle healthchecks by this time at startup.
    #[serde(default = "General::idle_healthcheck_delay")]
    pub idle_healthcheck_delay: u64,
    /// Healthcheck timeout.
    #[serde(default = "General::healthcheck_timeout")]
    pub healthcheck_timeout: u64,
    /// HTTP health check port.
    pub healthcheck_port: Option<u16>,
    /// Maximum duration of a ban.
    #[serde(default = "General::ban_timeout")]
    pub ban_timeout: u64,
    /// Rollback timeout.
    #[serde(default = "General::rollback_timeout")]
    pub rollback_timeout: u64,
    /// Load balancing strategy.
    #[serde(default = "General::load_balancing_strategy")]
    pub load_balancing_strategy: LoadBalancingStrategy,
    /// How aggressive should the query parser be in determining reads.
    #[serde(default)]
    pub read_write_strategy: ReadWriteStrategy,
    /// Read write split.
    #[serde(default)]
    pub read_write_split: ReadWriteSplit,
    /// TLS certificate.
    pub tls_certificate: Option<PathBuf>,
    /// TLS private key.
    pub tls_private_key: Option<PathBuf>,
    #[serde(default)]
    pub tls_client_required: bool,
    /// TLS verification mode (for connecting to servers)
    #[serde(default = "General::default_tls_verify")]
    pub tls_verify: TlsVerifyMode,
    /// TLS CA certificate (for connecting to servers).
    pub tls_server_ca_certificate: Option<PathBuf>,
    /// Shutdown timeout.
    #[serde(default = "General::default_shutdown_timeout")]
    pub shutdown_timeout: u64,
    /// Shutdown termination timeout (after shutdown_timeout expires, forcibly terminate).
    #[serde(default = "General::default_shutdown_termination_timeout")]
    pub shutdown_termination_timeout: Option<u64>,
    /// Broadcast IP.
    pub broadcast_address: Option<Ipv4Addr>,
    /// Broadcast port.
    #[serde(default = "General::broadcast_port")]
    pub broadcast_port: u16,
    /// Load queries to file (warning: slow, don't use in production).
    #[serde(default)]
    pub query_log: Option<PathBuf>,
    /// Enable OpenMetrics server on this port.
    pub openmetrics_port: Option<u16>,
    /// OpenMetrics prefix.
    pub openmetrics_namespace: Option<String>,
    /// Prepared statatements support.
    #[serde(default)]
    pub prepared_statements: PreparedStatements,
    /// Parse Queries override.
    #[serde(default = "General::query_parser_enabled")]
    pub query_parser_enabled: bool,
    /// Query parser.
    #[serde(default)]
    pub query_parser: QueryParserLevel,
    /// Query parser engine.
    #[serde(default)]
    pub query_parser_engine: QueryParserEngine,
    /// Limit on the number of prepared statements in the server cache.
    #[serde(default = "General::prepared_statements_limit")]
    pub prepared_statements_limit: usize,
    #[serde(default = "General::query_cache_limit")]
    pub query_cache_limit: usize,
    /// Automatically add connection pools for user/database pairs we don't have.
    #[serde(default = "General::default_passthrough_auth")]
    pub passthrough_auth: PassthoughAuth,
    /// Server connect timeout.
    #[serde(default = "General::default_connect_timeout")]
    pub connect_timeout: u64,
    /// Attempt connections multiple times on bad networks.
    #[serde(default = "General::connect_attempts")]
    pub connect_attempts: u64,
    /// How long to wait between connection attempts.
    #[serde(default = "General::default_connect_attempt_delay")]
    pub connect_attempt_delay: u64,
    /// How long to wait for a query to return the result before aborting. Dangerous: don't use unless your network is bad.
    #[serde(default = "General::default_query_timeout")]
    pub query_timeout: u64,
    /// Checkout timeout.
    #[serde(default = "General::checkout_timeout")]
    pub checkout_timeout: u64,
    /// Login timeout.
    #[serde(default = "General::client_login_timeout")]
    pub client_login_timeout: u64,
    /// Dry run for sharding. Parse the query, route to shard 0.
    #[serde(default)]
    pub dry_run: bool,
    /// Idle timeout.
    #[serde(default = "General::idle_timeout")]
    pub idle_timeout: u64,
    /// Client idle timeout.
    #[serde(default = "General::default_client_idle_timeout")]
    pub client_idle_timeout: u64,
    /// Client idle in transaction timeout.
    #[serde(default = "General::default_client_idle_in_transaction_timeout")]
    pub client_idle_in_transaction_timeout: u64,
    /// Server lifetime.
    #[serde(default = "General::server_lifetime")]
    pub server_lifetime: u64,
    /// Mirror queue size.
    #[serde(default = "General::mirror_queue")]
    pub mirror_queue: usize,
    /// Mirror exposure
    #[serde(default = "General::mirror_exposure")]
    pub mirror_exposure: f32,
    #[serde(default)]
    pub auth_type: AuthType,
    /// Disable cross-shard queries.
    #[serde(default)]
    pub cross_shard_disabled: bool,
    /// How often to refresh DNS entries, in ms.
    #[serde(default)]
    pub dns_ttl: Option<u64>,
    /// LISTEN/NOTIFY channel size.
    #[serde(default)]
    pub pub_sub_channel_size: usize,
    /// Log client connections.
    #[serde(default = "General::log_connections")]
    pub log_connections: bool,
    /// Log client disconnections.
    #[serde(default = "General::log_disconnections")]
    pub log_disconnections: bool,
    /// Two-phase commit.
    #[serde(default)]
    pub two_phase_commit: bool,
    /// Two-phase commit automatic transactions.
    #[serde(default)]
    pub two_phase_commit_auto: Option<bool>,
    /// Enable expanded EXPLAIN output.
    #[serde(default = "General::expanded_explain")]
    pub expanded_explain: bool,
    /// Stats averaging period (in milliseconds).
    #[serde(default = "General::stats_period")]
    pub stats_period: u64,
    /// Connection cleanup algorithm.
    #[serde(default = "General::connection_recovery")]
    pub connection_recovery: ConnectionRecovery,
    /// Client connection recovery
    #[serde(default = "General::client_connection_recovery")]
    pub client_connection_recovery: ConnectionRecovery,
    /// LSN check interval.
    #[serde(default = "General::lsn_check_interval")]
    pub lsn_check_interval: u64,
    /// LSN check timeout.
    #[serde(default = "General::lsn_check_timeout")]
    pub lsn_check_timeout: u64,
    /// LSN check delay.
    #[serde(default = "General::lsn_check_delay")]
    pub lsn_check_delay: u64,
    /// Minimum ID for unique ID generator.
    #[serde(default)]
    pub unique_id_min: u64,
    /// System catalogs are omnisharded?
    #[serde(default = "General::default_system_catalogs")]
    pub system_catalogs: SystemCatalogsBehavior,
    /// Omnisharded queries are sticky by default.
    #[serde(default)]
    pub omnisharded_sticky: bool,
    /// Copy format used for resharding.
    #[serde(default)]
    pub resharding_copy_format: CopyFormat,
    /// Trigger a schema reload on DDL like CREATE TABLE.
    #[serde(default = "General::reload_schema_on_ddl")]
    pub reload_schema_on_ddl: bool,
    /// Load database schema.
    #[serde(default = "General::load_schema")]
    pub load_schema: LoadSchema,
    /// Cross-shard backend.
    #[serde(default = "General::cross_shard_backend")]
    pub cross_shard_backend: CrossShardBackend,
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
            cross_shard_backend: Self::cross_shard_backend(),
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

    fn cross_shard_backend() -> CrossShardBackend {
        Self::env_enum_or_default("PGDOG_CROSS_SHARD_BACKEND")
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
