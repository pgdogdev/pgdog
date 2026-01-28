use tokio::time::Instant;

use crate::net::messages::BackendKeyData;

/// Connection request.
#[derive(Clone, Debug, Copy)]
pub struct Request {
    pub id: BackendKeyData,
    pub created_at: Instant,
    pub read: bool,
}

impl Request {
    pub fn new(id: BackendKeyData, read: bool) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read,
        }
    }

    pub fn unrouted(id: BackendKeyData) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read: false,
        }
    }
}

impl Default for Request {
    fn default() -> Self {
        Self::unrouted(BackendKeyData::new())
    }
}
