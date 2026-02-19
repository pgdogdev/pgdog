//! Pool configuration.

use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use pgdog_config::Role;
use serde::{Deserialize, Serialize};

use crate::config::{Database, General, User};

/// Pool configuration.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub struct Config {
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
    /// Connect timeout duration.
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Checkout timeout duration.
    pub fn checkout_timeout(&self) -> Duration {
        self.checkout_timeout
    }

    /// DNS TTL duration.
    pub fn dns_ttl(&self) -> Duration {
        self.dns_ttl
    }

    /// Idle timeout duration.
    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    /// Max age duration.
    pub fn max_age(&self) -> Duration {
        self.max_age
    }

    /// Healthcheck timeout.
    pub fn healthcheck_timeout(&self) -> Duration {
        self.healthcheck_timeout
    }

    /// How long to wait between healtchecks.
    pub fn healthcheck_interval(&self) -> Duration {
        self.healthcheck_interval
    }

    /// Idle healtcheck interval.
    pub fn idle_healthcheck_interval(&self) -> Duration {
        self.idle_healthcheck_interval
    }

    /// Idle healtcheck delay.
    pub fn idle_healthcheck_delay(&self) -> Duration {
        self.idle_healthcheck_delay
    }

    /// Ban timeout.
    pub fn ban_timeout(&self) -> Duration {
        self.ban_timeout
    }

    /// Rollback timeout.
    pub fn rollback_timeout(&self) -> Duration {
        self.rollback_timeout
    }

    /// Read timeout.
    pub fn read_timeout(&self) -> Duration {
        self.read_timeout
    }

    pub fn query_timeout(&self) -> Duration {
        self.query_timeout
    }

    /// Create from database/user configuration.
    pub fn new(general: &General, database: &Database, user: &User, is_only_replica: bool) -> Self {
        Self {
            inner: pgdog_stats::Config {
                min: user
                    .min_pool_size
                    .unwrap_or(database.min_pool_size.unwrap_or(general.min_pool_size)),
                max: user
                    .pool_size
                    .unwrap_or(database.pool_size.unwrap_or(general.default_pool_size)),
                max_age: Duration::from_millis(
                    user.server_lifetime
                        .unwrap_or(database.server_lifetime.unwrap_or(general.server_lifetime)),
                ),
                healthcheck_interval: Duration::from_millis(general.healthcheck_interval),
                idle_healthcheck_interval: Duration::from_millis(general.idle_healthcheck_interval),
                idle_healthcheck_delay: Duration::from_millis(general.idle_healthcheck_delay),
                healthcheck_timeout: Duration::from_millis(general.healthcheck_timeout),
                ban_timeout: Duration::from_millis(general.ban_timeout),
                rollback_timeout: Duration::from_millis(general.rollback_timeout),
                statement_timeout: user
                    .statement_timeout
                    .or(database.statement_timeout)
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
                read_only: user
                    .read_only
                    .unwrap_or(database.read_only.unwrap_or_default()),
                prepared_statements_limit: general.prepared_statements_limit,
                stats_period: Duration::from_millis(general.stats_period),
                bannable: !is_only_replica,
                connection_recovery: general.connection_recovery,
                lsn_check_interval: Duration::from_millis(general.lsn_check_interval),
                lsn_check_timeout: Duration::from_millis(general.lsn_check_timeout),
                lsn_check_delay: Duration::from_millis(general.lsn_check_delay),
                role_detection: database.role == Role::Auto,
                ..Default::default()
            },
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            inner: pgdog_stats::Config::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
    fn test_user_takes_precedence_over_database() {
        let general = General::default();
        let user = User {
            pool_size: Some(5),
            ..Default::default()
        };
        let database = Database {
            pool_size: Some(10),
            ..Default::default()
        };

        let config = Config::new(&general, &database, &user, false);

        assert_eq!(5, config.max);
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
