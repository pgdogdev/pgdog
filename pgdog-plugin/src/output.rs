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

    /// # Safety
    ///
    /// Don't use this function unless you're cleaning up plugin
    /// output.
    pub unsafe fn deallocate(&self) {
        #[allow(non_upper_case_globals)]
        if self.decision == RoutingDecision_FORWARD {
            self.output.route.drop();
        }
    }
}
