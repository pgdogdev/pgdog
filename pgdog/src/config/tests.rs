#[cfg(test)]
pub mod test {
    use std::env;
    use std::path::PathBuf;
    use std::time::Duration;

    use crate::backend::databases::init;

    use super::super::*;

    pub fn load_test() {
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

    pub fn load_test_replicas() {
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
        assert!(
            config.prepared_statements(),
            "Prepared statements should be enabled in transaction mode"
        );

        // Test session mode - prepared statements should be disabled
        config.config.general.pooler_mode = PoolerMode::Session;
        config.config.general.prepared_statements = PreparedStatements::Extended;
        assert!(
            !config.prepared_statements(),
            "Prepared statements should be disabled in session mode"
        );

        // Test session mode with full prepared statements - should still be disabled
        config.config.general.pooler_mode = PoolerMode::Session;
        config.config.general.prepared_statements = PreparedStatements::Full;
        assert!(
            !config.prepared_statements(),
            "Prepared statements should be disabled in session mode even when set to Full"
        );

        // Test transaction mode with disabled prepared statements - should remain disabled
        config.config.general.pooler_mode = PoolerMode::Transaction;
        config.config.general.prepared_statements = PreparedStatements::Disabled;
        assert!(!config.prepared_statements(), "Prepared statements should remain disabled when explicitly set to Disabled in transaction mode");
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
        env::set_var("PGDOG_CONNECT_ATTEMPT_DELAY", "1000");
        env::set_var("PGDOG_QUERY_TIMEOUT", "30000");
        env::set_var("PGDOG_CLIENT_IDLE_TIMEOUT", "3600000");

        assert_eq!(General::idle_healthcheck_interval(), 45000);
        assert_eq!(General::idle_healthcheck_delay(), 10000);
        assert_eq!(General::ban_timeout(), 600000);
        assert_eq!(General::rollback_timeout(), 10000);
        assert_eq!(General::default_shutdown_timeout(), 120000);
        assert_eq!(General::default_connect_attempt_delay(), 1000);
        assert_eq!(General::default_query_timeout(), 30000);
        assert_eq!(General::default_client_idle_timeout(), 3600000);

        env::remove_var("PGDOG_IDLE_HEALTHCHECK_INTERVAL");
        env::remove_var("PGDOG_IDLE_HEALTHCHECK_DELAY");
        env::remove_var("PGDOG_BAN_TIMEOUT");
        env::remove_var("PGDOG_ROLLBACK_TIMEOUT");
        env::remove_var("PGDOG_SHUTDOWN_TIMEOUT");
        env::remove_var("PGDOG_CONNECT_ATTEMPT_DELAY");
        env::remove_var("PGDOG_QUERY_TIMEOUT");
        env::remove_var("PGDOG_CLIENT_IDLE_TIMEOUT");

        assert_eq!(General::idle_healthcheck_interval(), 30000);
        assert_eq!(General::idle_healthcheck_delay(), 5000);
        assert_eq!(General::ban_timeout(), 300000);
        assert_eq!(General::rollback_timeout(), 5000);
        assert_eq!(General::default_shutdown_timeout(), 60000);
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
        assert_eq!(General::prepared_statements_limit(), usize::MAX);
        assert_eq!(General::query_cache_limit(), usize::MAX);
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
