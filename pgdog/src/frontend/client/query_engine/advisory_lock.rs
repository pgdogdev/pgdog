use fnv::FnvHashSet;

use crate::frontend::router::parser::statement::{AdvisoryLocks as ParserAdvisoryLocks, LockScope};

/// Tracks advisory locks held by the current client across requests.
#[derive(Default, Debug)]
pub(crate) struct AdvisoryLocks {
    locks: FnvHashSet<i64>,
    xact_locks: FnvHashSet<i64>,
}

impl AdvisoryLocks {
    pub(crate) fn merge(&mut self, locks: &ParserAdvisoryLocks, in_transaction: bool) {
        for lock in locks.iter() {
            if lock.unlock {
                // Only session-scoped locks can be released by name — xact locks
                // are released by Postgres at COMMIT/ROLLBACK.
                if let Some(id) = lock.id {
                    self.locks.remove(&id);
                } else {
                    // pg_advisory_unlock_all() clears every session lock.
                    self.locks.clear();
                }
            } else if let Some(id) = lock.id {
                match lock.scope {
                    LockScope::Session => {
                        self.locks.insert(id);
                    }
                    LockScope::Transaction => {
                        // xact locks outside a transaction run in an implicit
                        // transaction that commits immediately after the
                        // statement, no tracking required.
                        if in_transaction {
                            self.xact_locks.insert(id);
                        }
                    }
                }
            }
        }
    }

    pub fn commit(&mut self) {
        self.xact_locks.clear();
    }

    pub fn rollback(&mut self) {
        self.xact_locks.clear();
    }

    pub fn locked(&self) -> bool {
        !self.locks.is_empty() || !self.xact_locks.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn session_contains(&self, id: i64) -> bool {
        self.locks.contains(&id)
    }

    #[cfg(test)]
    pub(crate) fn xact_contains(&self, id: i64) -> bool {
        self.xact_locks.contains(&id)
    }

    #[cfg(test)]
    pub(crate) fn session_len(&self) -> usize {
        self.locks.len()
    }

    #[cfg(test)]
    pub(crate) fn xact_len(&self) -> usize {
        self.xact_locks.len()
    }
}
