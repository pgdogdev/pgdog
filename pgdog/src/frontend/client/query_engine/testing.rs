use super::*;

impl QueryEngine {
    pub(crate) fn router(&mut self) -> &mut Router {
        &mut self.router
    }

    pub(crate) fn stats(&mut self) -> &mut Stats {
        &mut self.stats
    }

    pub(crate) fn advisory_locks(&mut self) -> &mut AdvisoryLocks {
        &mut self.advisory_locks
    }
}
