use std::net::SocketAddr;
use std::time::SystemTime;

use super::Stats;

/// Connected client.
#[derive(Copy, Clone, Debug)]
pub struct ConnectedClient {
    pub stats: Stats,
    pub addr: SocketAddr,
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
