//! Pool configuration.

use std::time::Duration;

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
    pub checkout_timeout: u64, // ms
    /// Close connections that have been idle for longer than this.
    pub idle_timeout: u64, // ms
    /// How long to wait for connections to be created.
    pub connect_timeout: u64, // ms
    /// How long a connection can be open.
    pub max_age: u64,
    /// Can this pool be banned from serving traffic?
    pub bannable: bool,
    /// Healtheck timeout.
    pub healthcheck_timeout: u64, // ms
    /// Healtcheck interval.
    pub healthcheck_interval: u64, // ms
    /// Idle healthcheck interval.
    pub idle_healthcheck_interval: u64, // ms
    /// Idle healthcheck delay.
    pub idle_healthcheck_delay: u64, // ms
    /// Read timeout (dangerous).
    pub read_timeout: u64, // ms
    /// Write timeout (dangerous).
    pub write_timeout: u64, // ms
    /// Query timeout (dangerous).
    pub query_timeout: u64, // ms
    /// Max ban duration.
    pub ban_timeout: u64, // ms
    /// Rollback timeout for dirty connections.
    pub rollback_timeout: u64,
    /// Statement timeout
    pub statement_timeout: Option<u64>,
    /// Replication mode.
    pub replication_mode: bool,
    /// Pooler mode.
    pub pooler_mode: PoolerMode,
}

impl Config {
    /// Connect timeout duration.
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.checkout_timeout)
    }

    /// Checkout timeout duration.
    pub fn checkout_timeout(&self) -> Duration {
        Duration::from_millis(self.checkout_timeout)
    }

    /// Idle timeout duration.
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_millis(self.idle_timeout)
    }

    /// Max age duration.
    pub fn max_age(&self) -> Duration {
        Duration::from_millis(self.max_age)
    }

    /// Healthcheck timeout.
    pub fn healthcheck_timeout(&self) -> Duration {
        Duration::from_millis(self.healthcheck_timeout)
    }

    /// How long to wait between healtchecks.
    pub fn healthcheck_interval(&self) -> Duration {
        Duration::from_millis(self.healthcheck_interval)
    }

    /// Idle healtcheck interval.
    pub fn idle_healthcheck_interval(&self) -> Duration {
        Duration::from_millis(self.idle_healthcheck_interval)
    }

    /// Idle healtcheck delay.
    pub fn idle_healthcheck_delay(&self) -> Duration {
        Duration::from_millis(self.idle_healthcheck_delay)
    }

    /// Ban timeout.
    pub fn ban_timeout(&self) -> Duration {
        Duration::from_millis(self.ban_timeout)
    }

    /// Rollback timeout.
    pub fn rollback_timeout(&self) -> Duration {
        Duration::from_millis(self.rollback_timeout)
    }

    /// Read timeout.
    pub fn read_timeout(&self) -> Duration {
        Duration::from_millis(self.read_timeout)
    }

    pub fn query_timeout(&self) -> Duration {
        Duration::from_millis(self.query_timeout)
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
    pub fn new(general: &General, _database: &Database, user: &User) -> Self {
        Config {
            min: user.min_pool_size.unwrap_or(general.min_pool_size),
            max: user.pool_size.unwrap_or(general.default_pool_size),
            healthcheck_interval: general.healthcheck_interval,
            idle_healthcheck_interval: general.idle_healthcheck_interval,
            idle_healthcheck_delay: general.idle_healthcheck_delay,
            ban_timeout: general.ban_timeout,
            rollback_timeout: general.rollback_timeout,
            statement_timeout: user.statement_timeout,
            replication_mode: user.replication_mode,
            pooler_mode: user.pooler_mode.unwrap_or(general.pooler_mode),
            connect_timeout: general.connect_timeout,
            query_timeout: general.query_timeout,
            checkout_timeout: general.checkout_timeout,
            ..Default::default()
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            min: 1,
            max: 10,
            checkout_timeout: 5_000,
            idle_timeout: 60_000,
            connect_timeout: 5_000,
            max_age: 24 * 3600 * 1000,
            bannable: true,
            healthcheck_timeout: 5_000,
            healthcheck_interval: 30_000,
            idle_healthcheck_interval: 5_000,
            idle_healthcheck_delay: 5_000,
            read_timeout: Duration::MAX.as_millis() as u64,
            write_timeout: Duration::MAX.as_millis() as u64,
            query_timeout: Duration::MAX.as_millis() as u64,
            ban_timeout: Duration::from_secs(300).as_millis() as u64,
            rollback_timeout: Duration::from_secs(5).as_millis() as u64,
            statement_timeout: None,
            replication_mode: false,
            pooler_mode: PoolerMode::default(),
        }
    }
}
