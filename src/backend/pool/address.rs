//! Server address.

use serde::{Deserialize, Serialize};

/// Server address.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Address {
    /// Server host.
    pub host: String,
    /// Server port.
    pub port: u16,
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}
