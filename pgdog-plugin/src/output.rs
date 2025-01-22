//! Plugin output helpers.
#![allow(non_upper_case_globals)]
use crate::bindings::*;

impl std::fmt::Debug for Output {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Output")
            .field("decision", &self.decision)
            .finish()
    }
}

impl Output {
    /// Plugin doesn't want to deal with the input.
    /// Router will skip it.
    pub fn skip() -> Self {
        Self {
            decision: RoutingDecision_NO_DECISION,
            output: RoutingOutput::new_route(Route::unknown()),
        }
    }

    /// Create new forward output.
    ///
    /// This means the query will be forwarded as-is to a destination
    /// specified in the route.
    pub fn new_forward(route: Route) -> Output {
        Output {
            decision: RoutingDecision_FORWARD,
            output: RoutingOutput::new_route(route),
        }
    }

    /// Create new copy statement.
    pub fn new_copy(copy: Copy) -> Output {
        Output {
            decision: RoutingDecision_COPY,
            output: RoutingOutput::new_copy(copy),
        }
    }

    /// Sharded copy rows.
    pub fn new_copy_rows(output: CopyOutput) -> Output {
        Output {
            decision: RoutingDecision_COPY_ROWS,
            output: RoutingOutput::new_copy_rows(output),
        }
    }

    /// Get route determined by the plugin.
    pub fn route(&self) -> Option<Route> {
        match self.decision {
            RoutingDecision_FORWARD => Some(unsafe { self.output.route }),
            _ => None,
        }
    }

    /// Get copy info determined by the plugin.
    pub fn copy(&self) -> Option<Copy> {
        if self.decision == RoutingDecision_COPY {
            Some(unsafe { self.output.copy })
        } else {
            None
        }
    }

    /// Get copy rows if any.
    pub fn copy_rows(&self) -> Option<CopyOutput> {
        if self.decision == RoutingDecision_COPY_ROWS {
            Some(unsafe { self.output.copy_rows })
        } else {
            None
        }
    }

    /// # Safety
    ///
    /// Don't use this function unless you're cleaning up plugin
    /// output.
    pub unsafe fn deallocate(&self) {
        if self.decision == RoutingDecision_FORWARD {
            self.output.route.deallocate();
        }
        if self.decision == RoutingDecision_COPY {
            self.output.copy.deallocate();
        }
        if self.decision == RoutingDecision_COPY_ROWS {
            self.output.copy_rows.deallocate();
        }
    }
}
