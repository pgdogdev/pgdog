use std::net::SocketAddr;
use std::time::SystemTime;

use super::Stats;

/// Connected client.
#[derive(Copy, Clone, Debug)]
pub struct ConnectedClient {
    /// Client statistics.
    pub stats: Stats,
    /// Client IP address.
    pub addr: SocketAddr,
    /// System time when the client connected.
    pub connected_at: SystemTime,
}

impl ConnectedClient {
    /// New connected client.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            stats: Stats::new(),
            addr,
            connected_at: SystemTime::now(),
        }
    }
}
