use tokio::time::Instant;

use crate::net::messages::BackendKeyData;

/// Connection request.
#[derive(Clone, Debug, Copy)]
pub(crate) struct Request {
    pub(crate) id: BackendKeyData,
    pub(crate) created_at: Instant,
}

impl Request {
    pub(crate) fn new(id: BackendKeyData) -> Self {
        Self {
            id,
            created_at: Instant::now(),
        }
    }
}

impl Default for Request {
    fn default() -> Self {
        Self::new(BackendKeyData::new())
    }
}
