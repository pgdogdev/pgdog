//! Pool configuration.

use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use pgdog_config::Role;
use serde::{Deserialize, Serialize};

use crate::config::{Database, General, User};

/// Pool configuration.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Default)]
pub(crate) struct Config {
    pub(crate) inner: pgdog_stats::Config,
}

impl Deref for Config {
    type Target = pgdog_stats::Config;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Config {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Config {
    /// Create from database/user configuration.
    pub(crate) fn new(general: &General, database: &Database, user: &User, is_only_replica: bool) -> Self {
        Self {
            inner: pgdog_stats::Config {
                min: user
                    .min_pool_size
                    .unwrap_or(database.min_pool_size.unwrap_or(general.min_pool_size)),
                min_primary: if let Some(user_setting) = user.role_config.min_pool_size_primary {
                    Some(user_setting)
                } else {
                    database.role_config.min_pool_size_primary
                },
                min_replica: if let Some(user_setting) = user.role_config.min_pool_size_replica {
                    Some(user_setting)
                } else {
                    database.role_config.min_pool_size_replica
                },
                max: user
                    .pool_size
                    .unwrap_or(database.pool_size.unwrap_or(general.default_pool_size)),
                max_primary: if let Some(user_setting) = user.role_config.pool_size_primary {
                    Some(user_setting)
                } else {
                    database.role_config.pool_size_primary
                },
                max_replica: if let Some(user_setting) = user.role_config.pool_size_replica {
                    Some(user_setting)
                } else {
                    database.role_config.pool_size_replica
                },
                max_age: Duration::from_millis(
                    user.server_lifetime
                        .unwrap_or(database.server_lifetime.unwrap_or(general.server_lifetime)),
                ),
                max_age_jitter: Duration::from_millis(
                    user.server_lifetime_jitter.unwrap_or(
                        database
                            .server_lifetime_jitter
                            .unwrap_or(general.server_lifetime_jitter),
                    ),
                ),
                healthcheck_interval: Duration::from_millis(general.healthcheck_interval),
                idle_healthcheck_interval: Duration::from_millis(general.idle_healthcheck_interval),
                idle_healthcheck_delay: Duration::from_millis(general.idle_healthcheck_delay),
                healthcheck_timeout: Duration::from_millis(general.healthcheck_timeout),
                require_healthcheck_on_discovery: general.require_healthcheck_on_discovery,
                ban_timeout: Duration::from_millis(general.ban_timeout),
                rollback_timeout: Duration::from_millis(general.rollback_timeout),
                statement_timeout: user
                    .statement_timeout
                    .or(database.statement_timeout)
                    .map(Duration::from_millis),
                lock_timeout: user
                    .lock_timeout
                    .or(database.lock_timeout)
                    .map(Duration::from_millis),
                replication_mode: user.replication_mode,
                pooler_mode: user
                    .pooler_mode
                    .unwrap_or(database.pooler_mode.unwrap_or(general.pooler_mode)),
                connect_timeout: Duration::from_millis(general.connect_timeout),
                connect_attempts: general.connect_attempts,
                connect_attempt_delay: general.connect_attempt_delay(),
                query_timeout: Duration::from_millis(general.query_timeout),
                checkout_timeout: Duration::from_millis(general.checkout_timeout),
                idle_timeout: Duration::from_millis(
                    user.idle_timeout
                        .unwrap_or(database.idle_timeout.unwrap_or(general.idle_timeout)),
                ),
                idle_timeout_primary: if let Some(user_setting) =
                    user.role_config.idle_timeout_primary
                {
                    Some(Duration::from_millis(user_setting))
                } else {
                    database
                        .role_config
                        .idle_timeout_primary
                        .map(Duration::from_millis)
                },
                idle_timeout_replica: if let Some(user_setting) =
                    user.role_config.idle_timeout_replica
                {
                    Some(Duration::from_millis(user_setting))
                } else {
                    database
                        .role_config
                        .idle_timeout_replica
                        .map(Duration::from_millis)
                },
                read_only: user
                    .read_only
                    .unwrap_or(database.read_only.unwrap_or_default()),
                prepared_statements: pgdog_stats::PreparedStatementsConfig {
                    level: general.prepared_statements,
                    limit: general.prepared_statements_limit,
                    ttl: general.prepared_statements_ttl(),
                    ttl_jitter: general.prepared_statements_ttl_jitter(),
                },
                stats_period: Duration::from_millis(general.stats_period),
                bannable: !is_only_replica,
                connection_recovery: general.connection_recovery,
                lsn_check_interval: Duration::from_millis(general.lsn_check_interval),
                lsn_check_timeout: Duration::from_millis(general.lsn_check_timeout),
                lsn_check_delay: Duration::from_millis(general.lsn_check_delay),
                role_detection: database.role == Role::Auto,
                resharding_only: database.resharding_only,
                lb_weight: database.lb_weight,
                ..Default::default()
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use pgdog_config::PoolerMode;

    fn create_database(role: Role) -> Database {
        Database {
            name: "test".into(),
            role,
            host: "localhost".into(),
            port: 5432,
            ..Default::default()
        }
    }

    #[test]
    fn test_role_auto_enables_role_detection() {
        let general = General::default();
        let user = User::default();
        let database = create_database(Role::Auto);

        let config = Config::new(&general, &database, &user, false);

        assert!(config.role_detection);
    }

    #[test]
    fn test_prepared_statements_config_from_general() {
        let general = General {
            prepared_statements_ttl: Some(60_000),
            prepared_statements_limit: 10,
            ..Default::default()
        };

        let config = Config::new(
            &general,
            &create_database(Role::Primary),
            &User::default(),
            false,
        );

        assert_eq!(
            config.prepared_statements.ttl,
            Some(Duration::from_millis(60_000))
        );
        assert_eq!(config.prepared_statements.limit, 10);
        assert_eq!(
            config.prepared_statements.level,
            general.prepared_statements
        );
    }

    #[test]
    fn test_user_takes_precedence_over_database() {
        let general = General::default();
        let user = User {
            pool_size: Some(5),
            min_pool_size: Some(5),
            server_lifetime: Some(5),
            server_lifetime_jitter: Some(1),
            statement_timeout: Some(5),
            lock_timeout: Some(7),
            pooler_mode: Some(PoolerMode::Session),
            idle_timeout: Some(5),
            read_only: Some(true),
            ..Default::default()
        };

        let database = Database {
            pool_size: Some(10),
            min_pool_size: Some(10),
            server_lifetime: Some(10),
            server_lifetime_jitter: Some(2),
            statement_timeout: Some(10),
            lock_timeout: Some(11),
            pooler_mode: Some(PoolerMode::Transaction),
            idle_timeout: Some(10),
            read_only: Some(false),
            ..Default::default()
        };

        let config = Config::new(&general, &database, &user, false);

        assert_eq!(5, config.max);
        assert_eq!(5, config.min);
        assert_eq!(Duration::from_millis(5), config.max_age);
        assert_eq!(Duration::from_millis(1), config.max_age_jitter);
        assert_eq!(Some(Duration::from_millis(5)), config.statement_timeout);
        assert_eq!(Some(Duration::from_millis(7)), config.lock_timeout);
        assert_eq!(PoolerMode::Session, config.pooler_mode);
        assert_eq!(Duration::from_millis(5), config.idle_timeout);
        assert!(config.read_only);
    }

    #[test]
    fn test_role_specific_pool_config_user_takes_precedence_over_database() {
        let general = General::default();
        let mut user = User::default();
        user.role_config.min_pool_size_primary = Some(2);
        user.role_config.idle_timeout_primary = Some(20);

        let mut database = Database::default();
        database.role_config.min_pool_size_primary = Some(3);
        database.role_config.min_pool_size_replica = Some(4);
        database.role_config.idle_timeout_primary = Some(30);
        database.role_config.idle_timeout_replica = Some(40);

        let config = Config::new(&general, &database, &user, false);

        assert_eq!(config.min_primary, Some(2));
        assert_eq!(config.min_replica, Some(4));
        assert_eq!(config.idle_timeout_primary, Some(Duration::from_millis(20)));
        assert_eq!(config.idle_timeout_replica, Some(Duration::from_millis(40)));
    }

    #[test]
    fn test_jitter_falls_through_general_to_database_to_user() {
        let general = General {
            server_lifetime_jitter: 100,
            ..General::default()
        };

        // Only general set: pool inherits the general value.
        let cfg = Config::new(&general, &Database::default(), &User::default(), false);
        assert_eq!(Duration::from_millis(100), cfg.max_age_jitter);

        // Database overrides general.
        let database = Database {
            server_lifetime_jitter: Some(200),
            ..Default::default()
        };
        let cfg = Config::new(&general, &database, &User::default(), false);
        assert_eq!(Duration::from_millis(200), cfg.max_age_jitter);

        // User overrides both.
        let user = User {
            server_lifetime_jitter: Some(300),
            ..Default::default()
        };
        let cfg = Config::new(&general, &database, &user, false);
        assert_eq!(Duration::from_millis(300), cfg.max_age_jitter);
    }

    #[test]
    fn test_jitter_default_is_zero() {
        let general = General::default();
        let cfg = Config::new(&general, &Database::default(), &User::default(), false);
        assert_eq!(Duration::ZERO, cfg.max_age_jitter);
    }

    #[test]
    fn test_role_primary_disables_role_detection() {
        let general = General::default();
        let user = User::default();
        let database = create_database(Role::Primary);

        let config = Config::new(&general, &database, &user, false);

        assert!(!config.role_detection);
    }

    #[test]
    fn test_role_replica_disables_role_detection() {
        let general = General::default();
        let user = User::default();
        let database = create_database(Role::Replica);

        let config = Config::new(&general, &database, &user, false);

        assert!(!config.role_detection);
    }
}
