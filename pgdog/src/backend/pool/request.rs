use tokio::time::Instant;

use crate::net::messages::FrontendPid;

/// Connection request.
#[derive(Clone, Debug, Copy)]
pub(crate) struct Request {
    pub(crate) id: FrontendPid,
    pub(crate) created_at: Instant,
    pub(crate) read: bool,

    // Load balancer uses this to determine if primary should be allowed to read.
    // Propagated from `User.read_only` setting.
    pub(crate) read_only: bool,
}

impl Request {
    pub(crate) fn new(id: FrontendPid, read: bool, read_only: bool) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read,
            read_only,
        }
    }

    pub(crate) fn unrouted(id: FrontendPid) -> Self {
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
