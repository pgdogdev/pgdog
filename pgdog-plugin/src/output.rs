//! Plugin output helpers.
use crate::bindings::*;

impl Output {
    /// Plugin doesn't want to deal with the input.
    /// Router will skip it.
    pub fn skip() -> Self {
        Self {
            decision: RoutingDecision_NO_DECISION,
            output: RoutingOutput::new_route(Route::unknown()),
        }
    }
}
