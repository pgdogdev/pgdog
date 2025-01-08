//! Query routing helpers.
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

    /// Get route determined by the plugin.
    pub fn route(&self) -> Option<Route> {
        match self.decision {
            RoutingDecision_FORWARD => Some(unsafe { self.output.route }),
            _ => None,
        }
    }
}

impl Route {
    /// The plugin has no idea what to do with this query.
    /// The router will ignore this and try another way.
    pub fn unknown() -> Route {
        Route {
            shard: Shard_ANY,
            affinity: Affinity_UNKNOWN,
        }
    }

    /// Read from any shard.
    pub fn read_any() -> Self {
        Self {
            affinity: Affinity_READ,
            shard: Shard_ANY,
        }
    }

    /// Read from any shard.
    pub fn write_any() -> Self {
        Self {
            affinity: Affinity_WRITE,
            shard: Shard_ANY,
        }
    }

    /// Is this a read?
    pub fn is_read(&self) -> bool {
        self.affinity == Affinity_READ
    }

    /// Is this a write?
    pub fn is_write(&self) -> bool {
        self.affinity == Affinity_WRITE
    }

    /// This query indicates a transaction a starting, e.g. BEGIN.
    pub fn is_transaction_start(&self) -> bool {
        self.affinity == Affinity_TRANSACTION_START
    }

    /// This query indicates a transaction is ending, e.g. COMMIT/ROLLBACK.
    pub fn is_transaction_end(&self) -> bool {
        self.affinity == Affinity_TRANSACTION_END
    }

    /// Which shard, if any.
    pub fn shard(&self) -> Option<usize> {
        if self.shard < 0 {
            None
        } else {
            Some(self.shard as usize)
        }
    }

    pub fn is_any_shard(&self) -> bool {
        self.shard == Shard_ANY
    }

    /// Query should go across all shards.
    pub fn is_cross_shard(&self) -> bool {
        self.shard == Shard_ALL
    }

    /// The plugin has no idea where to route this query.
    pub fn is_unknown(&self) -> bool {
        self.shard == Shard_ANY && self.affinity == Affinity_UNKNOWN
    }
}
