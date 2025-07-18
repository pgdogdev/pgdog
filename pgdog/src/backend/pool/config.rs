//! Pool configuration.

use std::{time::Duration, usize};

use serde::{Deserialize, Serialize};

use crate::config::{Database, General, PoolerMode, User};

/// Pool configuration.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub struct Config {
    /// Minimum connections that should be in the pool.
    pub min: usize,
    /// Maximum connections allowed in the pool.
    pub max: usize,
    /// How long to wait for a connection before giving up.
    pub checkout_timeout: Duration, // ms
    /// Interval duration of DNS cache refresh.
    pub dns_ttl: Duration, // ms
    /// Close connections that have been idle for longer than this.
    pub idle_timeout: Duration, // ms
    /// How long to wait for connections to be created.
    pub connect_timeout: Duration, // ms
    /// How many times to attempt a connection before returning an error.
    pub connect_attempts: u64,
    /// How long to wait between connection attempts.
    pub connect_attempt_delay: Duration,
    /// How long a connection can be open.
    pub max_age: Duration,
    /// Can this pool be banned from serving traffic?
    pub bannable: bool,
    /// Healtheck timeout.
    pub healthcheck_timeout: Duration, // ms
    /// Healtcheck interval.
    pub healthcheck_interval: Duration, // ms
    /// Idle healthcheck interval.
    pub idle_healthcheck_interval: Duration, // ms
    /// Idle healthcheck delay.
    pub idle_healthcheck_delay: Duration, // ms
    /// Read timeout (dangerous).
    pub read_timeout: Duration, // ms
    /// Write timeout (dangerous).
    pub write_timeout: Duration, // ms
    /// Query timeout (dangerous).
    pub query_timeout: Duration, // ms
    /// Max ban duration.
    pub ban_timeout: Duration, // ms
    /// Rollback timeout for dirty connections.
    pub rollback_timeout: Duration,
    /// Statement timeout
    pub statement_timeout: Option<Duration>,
    /// Replication mode.
    pub replication_mode: bool,
    /// Pooler mode.
    pub pooler_mode: PoolerMode,
    /// Read only mode.
    pub read_only: bool,
    /// Maximum prepared statements per connection.
    pub prepared_statements_limit: usize,
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

    /// Default config for a primary.
    ///
    /// The ban is ignored by the shard router
    /// if the primary is used for writes.
    ///
    /// The ban is taken into account if the primary
    /// is used for reads.
    pub fn default_primary() -> Self {
        Self {
            bannable: true,
            ..Default::default()
        }
    }

    /// Create from database/user configuration.
    pub fn new(general: &General, database: &Database, user: &User) -> Self {
        Config {
            min: database
                .min_pool_size
                .unwrap_or(user.min_pool_size.unwrap_or(general.min_pool_size)),
            max: database
                .pool_size
                .unwrap_or(user.pool_size.unwrap_or(general.default_pool_size)),
            healthcheck_interval: Duration::from_millis(general.healthcheck_interval),
            idle_healthcheck_interval: Duration::from_millis(general.idle_healthcheck_interval),
            idle_healthcheck_delay: Duration::from_millis(general.idle_healthcheck_delay),
            healthcheck_timeout: Duration::from_millis(general.healthcheck_timeout),
            ban_timeout: Duration::from_millis(general.ban_timeout),
            rollback_timeout: Duration::from_millis(general.rollback_timeout),
            statement_timeout: if let Some(statement_timeout) = database.statement_timeout {
                Some(statement_timeout)
            } else {
                user.statement_timeout
            }
            .map(Duration::from_millis),
            replication_mode: user.replication_mode,
            pooler_mode: database
                .pooler_mode
                .unwrap_or(user.pooler_mode.unwrap_or(general.pooler_mode)),
            connect_timeout: Duration::from_millis(general.connect_timeout),
            connect_attempts: general.connect_attempts,
            connect_attempt_delay: general.connect_attempt_delay(),
            query_timeout: Duration::from_millis(general.query_timeout),
            checkout_timeout: Duration::from_millis(general.checkout_timeout),
            idle_timeout: Duration::from_millis(
                user.idle_timeout
                    .unwrap_or(database.idle_timeout.unwrap_or(general.idle_timeout)),
            ),
            read_only: database
                .read_only
                .unwrap_or(user.read_only.unwrap_or_default()),
            prepared_statements_limit: general.prepared_statements_limit,
            ..Default::default()
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            min: 1,
            max: 10,
            checkout_timeout: Duration::from_millis(5_000),
            idle_timeout: Duration::from_millis(60_000),
            connect_timeout: Duration::from_millis(5_000),
            connect_attempts: 1,
            connect_attempt_delay: Duration::from_millis(10),
            max_age: Duration::from_millis(24 * 3600 * 1000),
            bannable: true,
            healthcheck_timeout: Duration::from_millis(5_000),
            healthcheck_interval: Duration::from_millis(30_000),
            idle_healthcheck_interval: Duration::from_millis(5_000),
            idle_healthcheck_delay: Duration::from_millis(5_000),
            read_timeout: Duration::MAX,
            write_timeout: Duration::MAX,
            query_timeout: Duration::MAX,
            ban_timeout: Duration::from_secs(300),
            rollback_timeout: Duration::from_secs(5),
            statement_timeout: None,
            replication_mode: false,
            pooler_mode: PoolerMode::default(),
            read_only: false,
            prepared_statements_limit: usize::MAX,
            dns_ttl: Duration::from_millis(60_000),
        }
    }
}
