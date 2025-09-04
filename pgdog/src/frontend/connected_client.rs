use chrono::{DateTime, Local};
use std::net::SocketAddr;

use crate::net::Parameters;

use super::Stats;

/// Connected client.
#[derive(Clone, Debug)]
pub(crate) struct ConnectedClient {
    /// Client statistics.
    pub(crate) stats: Stats,
    /// Client IP address.
    pub(crate) addr: SocketAddr,
    /// System time when the client connected.
    pub(crate) connected_at: DateTime<Local>,
    /// Client connection parameters.
    pub(crate) paramters: Parameters,
}

impl ConnectedClient {
    /// New connected client.
    pub(crate) fn new(addr: SocketAddr, params: &Parameters) -> Self {
        Self {
            stats: Stats::new(),
            addr,
            connected_at: Local::now(),
            paramters: params.clone(),
        }
    }
}
