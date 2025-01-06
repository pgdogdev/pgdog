//! Pool configuration.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::config::{Database, User};

/// Pool configuration.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
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
    pub fn idle_healtcheck_delay(&self) -> Duration {
        Duration::from_millis(self.idle_healthcheck_delay)
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
    pub fn new(database: &Database, user: &User) -> Self {
        todo!()
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
        }
    }
}
