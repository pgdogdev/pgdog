use tokio::time::Instant;

use crate::{frontend::router::Route, net::messages::FrontendPid};

/// Request for connection(s) from connection pool(s).
/// Contains all the necessary information for the load balancer / connection pool
/// to give the right connection to the client.
#[derive(Clone, Debug, Copy)]
pub(crate) struct Request {
    /// The id of the client. We use this to track
    /// which client is assigned to which server connection(s).
    pub(crate) id: FrontendPid,
    /// The request was created at this time. We push this down everywhere
    /// to avoid calling `Instant::now()` repetitively.
    pub(crate) created_at: Instant,
    /// Is the query a read or a write? This is the query router decision, i.e.,
    /// a read would typically be a SELECT.
    pub(crate) read: bool,
    /// Load balancer uses this to determine if primary should be allowed to read.
    /// Propagated from `User.read_only` setting.
    pub(crate) read_only: bool,
    /// Minimum replica LSN the client wants the replica to have
    /// before allowing reads on it, in its 64-bit representation.
    pub(crate) lsn: Option<u64>,
}

impl Request {
    /// Create new request for client given a query route.
    pub(crate) fn new(id: FrontendPid, route: &Route, read_only: bool) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read: route.is_read(),
            lsn: route.lsn,
            read_only,
        }
    }

    pub(crate) fn unrouted(id: FrontendPid) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read: false,
            read_only: false,
            lsn: None,
        }
    }
}

impl Default for Request {
    fn default() -> Self {
        Self::unrouted(FrontendPid::new())
    }
}
