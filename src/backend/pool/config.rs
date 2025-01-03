//! Pool configuration.

use serde::{Deserialize, Serialize};

/// Pool configuration.
#[derive(Debug, Serialize, Deserialize, Clone)]
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            min: 1,
            max: 10,
            checkout_timeout: 5_000,
            idle_timeout: 60_000,
            connect_timeout: 5_000,
        }
    }
}
