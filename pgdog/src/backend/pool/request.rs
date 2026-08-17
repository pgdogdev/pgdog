use tokio::time::Instant;

use crate::net::messages::FrontendPid;

/// Connection request.
#[derive(Clone, Debug, Copy)]
pub struct Request {
    pub id: FrontendPid,
    pub created_at: Instant,
    pub read: bool,

    // Load balancer uses this to determine if primary should be allowed to read.
    // Propagated from `User.read_only` setting.
    pub(crate) read_only: bool,
}

impl Request {
    pub fn new(id: FrontendPid, read: bool, read_only: bool) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read,
            read_only,
        }
    }

    pub fn unrouted(id: FrontendPid) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read: false,
            read_only: false,
        }
    }
}

impl Default for Request {
    fn default() -> Self {
        Self::unrouted(FrontendPid::new())
    }
}
