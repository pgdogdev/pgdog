use tokio::time::Instant;

use crate::net::messages::FrontendPid;

/// Connection request.
#[derive(Clone, Debug, Copy)]
pub struct Request {
    pub id: FrontendPid,
    pub created_at: Instant,
    pub read: bool,
    pub replica_only: bool,
}

impl Request {
    pub fn new(id: FrontendPid, read: bool) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read,
            replica_only: false,
        }
    }

    pub fn unrouted(id: FrontendPid) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read: false,
            replica_only: false,
        }
    }
}

impl Default for Request {
    fn default() -> Self {
        Self::unrouted(FrontendPid::new())
    }
}
