use chrono::{DateTime, Local};
use std::net::SocketAddr;

use crate::net::Parameters;

use super::Stats;

/// Connected client.
#[derive(Clone, Debug)]
pub struct ConnectedClient {
    /// Client statistics.
    pub stats: Stats,
    /// Client IP address.
    pub addr: SocketAddr,
    /// System time when the client connected.
    pub connected_at: DateTime<Local>,
    /// Client connection parameters.
    pub paramters: Parameters,
}

impl ConnectedClient {
    /// New connected client.
    pub fn new(addr: SocketAddr, params: &Parameters) -> Self {
        Self {
            stats: Stats::new(),
            addr,
            connected_at: Local::now(),
            paramters: params.clone(),
        }
    }
}
