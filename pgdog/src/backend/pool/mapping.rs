use crate::net::messages::BackendPid;

/// Mapping between a client and a server.
#[derive(Debug, Copy, Clone, PartialEq)]
pub(super) struct Mapping {
    /// Client ID.
    pub(super) client: BackendPid,
    /// Server ID.
    pub(super) server: BackendPid,
}
