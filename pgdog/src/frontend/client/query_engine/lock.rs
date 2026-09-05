use super::*;

impl QueryEngine {
    /// Check if we need to lock the backend to this client, and do so
    /// if needed.
    pub(super) fn check_lock(&mut self) {
        // The presence of advisory locks or manual pin
        // indicates we cannot release the backend.
        let locked = self.advisory_locks.locked()
            || !self.temp_tables.is_empty()
            || self
                .discarded_temp_tables
                .as_ref()
                .is_some_and(|tables| !tables.is_empty())
            || self.manual_lock;

        self.backend.lock(locked);
        self.stats.locked(locked);
    }
}
