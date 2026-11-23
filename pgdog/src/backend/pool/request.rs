use tokio::time::Instant;

use crate::{frontend::router::Route, net::messages::FrontendPid};

/// Request for a connection from a pool.
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
}

impl Request {
    /// Create new request for client given a query route.
    pub(crate) fn new(id: FrontendPid, route: &Route, created_at: Instant) -> Self {
        Self {
            id,
            created_at,
            read: route.is_read(),
        }
    }

    pub(crate) fn unrouted(id: FrontendPid) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read: false,
        }
    }
}

impl Default for Request {
    fn default() -> Self {
        Self::unrouted(FrontendPid::new())
    }
}
