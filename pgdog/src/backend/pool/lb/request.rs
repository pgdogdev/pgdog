use std::ops::Deref;

use crate::{backend::pool::Request as PoolRequest, frontend::router::Route, net::FrontendPid};
use tokio::time::Instant;

/// Pool checkout request with load balancing options.
#[derive(Debug, Clone, Default)]
pub(crate) struct LbRequest<'a> {
    request: PoolRequest,
    /// Whether the client is configured read-only.
    pub(super) read_only: bool,
    pub(super) label: Option<&'a str>,
    /// Minimum LSN a replica must reach before serving this request.
    pub(crate) lsn: Option<u64>,
}

impl<'a> LbRequest<'a> {
    /// Create new load balancer request.
    pub(crate) fn new(
        id: FrontendPid,
        route: &'a Route,
        read_only: bool,
        created_at: Instant,
    ) -> Self {
        Self {
            request: PoolRequest::new(id, route, created_at),
            label: route.label.as_deref(),
            lsn: route.lsn,
            read_only,
        }
    }

    /// Create a request for a client before a query route is available.
    pub(crate) fn unrouted(id: FrontendPid) -> Self {
        Self {
            request: PoolRequest::unrouted(id),
            read_only: false,
            label: None,
            lsn: None,
        }
    }
}

impl Deref for LbRequest<'_> {
    type Target = PoolRequest;

    fn deref(&self) -> &Self::Target {
        &self.request
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_borrows_route_label() {
        for label in [None, Some("analytics")] {
            let mut route = Route::default();
            route.label = label.map(str::to_owned);
            let request = LbRequest::new(FrontendPid::default(), &route, false, Instant::now());
            assert_eq!(request.label, label);
        }
    }
}
