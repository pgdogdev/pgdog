use chrono::{DateTime, Local};
use std::net::SocketAddr;

use crate::net::{Parameters, messages::BackendKeyData};

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
    /// Cancel key identifying this client and its secret.
    pub(crate) key: BackendKeyData,
}

impl ConnectedClient {
    /// New connected client.
    pub(crate) fn new(key: BackendKeyData, addr: SocketAddr, params: &Parameters) -> Self {
        Self {
            key,
            stats: Stats::new(),
            addr,
            connected_at: Local::now(),
            paramters: params.clone(),
        }
    }
}
