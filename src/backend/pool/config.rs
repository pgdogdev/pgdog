//! Pool configuration.

use std::time::Duration;

use serde::{Deserialize, Serialize};

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

    /// Default config for a primary.
    pub fn default_primary() -> Self {
        Self {
            bannable: false,
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
        }
    }
}
