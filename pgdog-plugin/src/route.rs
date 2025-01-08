#![allow(non_upper_case_globals)]

use crate::bindings::*;

impl RoutingOutput {
    /// Create new route.
    pub fn new_route(route: Route) -> RoutingOutput {
        RoutingOutput { route }
    }
}

impl Output {
    /// Create new forward output.
    ///
    /// This means the query will be forwarded as-is to a destination
    /// specified in the route.
    pub fn forward(route: Route) -> Output {
        Output {
            decision: RoutingDecision_FORWARD,
            output: RoutingOutput::new_route(route),
        }
    }

    pub fn route(&self) -> Option<Route> {
        match self.decision {
            RoutingDecision_FORWARD => Some(unsafe { self.output.route }),
            _ => None,
        }
    }
}

impl Route {
    ///
    pub fn unknown() -> Route {
        Route {
            shard: Shard_ANY,
            affinity: Affinity_UNKNOWN,
        }
    }

    /// Is this a read?
    pub fn read(&self) -> bool {
        self.affinity == Affinity_READ
    }

    /// Is this a write?
    pub fn write(&self) -> bool {
        self.affinity == Affinity_WRITE
    }

    /// Which shard, if any.
    pub fn shard(&self) -> Option<usize> {
        if self.shard < 0 {
            None
        } else {
            Some(self.shard as usize)
        }
    }

    pub fn any_shard(&self) -> bool {
        self.shard == Shard_ANY
    }

    /// Query should go across all shards.
    pub fn cross_shard(&self) -> bool {
        self.shard == Shard_ALL
    }

    /// The plugin has no idea where to route this query.
    pub fn is_unknown(&self) -> bool {
        self.shard == Shard_ANY && self.affinity == Affinity_UNKNOWN
    }
}
