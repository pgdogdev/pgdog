use crate::net::messages::BackendKeyData;

/// Mapping between a client and a server.
#[derive(Debug, Copy, Clone, PartialEq, Default)]
pub(super) struct Mapping {
    /// Client ID.
    pub(super) client: BackendKeyData,
    /// Server ID.
    pub(super) server: BackendKeyData,
}
