use super::*;

impl QueryEngine {
    pub fn router(&mut self) -> &mut Router {
        &mut self.router
    }

    pub fn stats(&mut self) -> &mut Stats {
        &mut self.stats
    }

    pub fn advisory_locks(&mut self) -> &mut AdvisoryLocks {
        &mut self.advisory_locks
    }
}
