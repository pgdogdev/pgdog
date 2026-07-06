use super::*;

impl QueryEngine {
    /// Check if we need to lock the backend to this client, and do so
    /// if needed.
    pub(super) fn check_lock(&mut self) {
        // The presence of advisory locks or manual pin
        // indicates we cannot release the backend.
        let locked = self.advisory_locks.locked() || self.manual_lock;

        self.backend.lock(self.advisory_locks.locked());
        self.stats.locked(locked);
    }
}
