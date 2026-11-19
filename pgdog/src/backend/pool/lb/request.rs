use std::ops::Deref;

use crate::{backend::pool::Request as PoolRequest, frontend::router::Route, net::FrontendPid};
use tokio::time::Instant;

/// Pool checkout request with load balancing options.
#[derive(Debug, Clone, Default)]
pub(crate) struct LbRequest {
    request: PoolRequest,
    /// Whether the client is configured read-only.
    pub(super) read_only: bool,
    pub(super) labels: Vec<String>,
}

impl LbRequest {
    /// Create new load balancer request.
    pub(crate) fn new(
        id: FrontendPid,
        route: &Route,
        labels: Vec<String>,
        read_only: bool,
        created_at: Instant,
    ) -> Self {
        Self {
            request: PoolRequest::new(id, route, created_at),
            labels,
            read_only,
        }
    }

    /// Create a request for a client before a query route is available.
    pub(crate) fn unrouted(id: FrontendPid) -> Self {
        Self {
            request: PoolRequest::unrouted(id),
            read_only: false,
            labels: Vec::new(),
        }
    }
}

impl Deref for LbRequest {
    type Target = PoolRequest;

    fn deref(&self) -> &Self::Target {
        &self.request
    }
}
