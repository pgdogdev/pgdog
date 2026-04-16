use fnv::FnvHashSet;

use crate::frontend::router::parser::statement::{AdvisoryLocks as ParserAdvisoryLocks, LockScope};

/// Tracks advisory locks held by the current client across requests.
#[derive(Default, Debug)]
pub(crate) struct AdvisoryLocks {
    locks: FnvHashSet<i64>,
}

impl AdvisoryLocks {
    pub(crate) fn merge(&mut self, locks: &ParserAdvisoryLocks) {
        for lock in locks.iter() {
            if lock.unlock {
                if let Some(id) = lock.id {
                    self.locks.remove(&id);
                } else {
                    // pg_advisory_unlock_all() clears every advisory lock.
                    self.locks.clear();
                }
            } else if let Some(id) = lock.id {
                if lock.scope == LockScope::Session {
                    self.locks.insert(id);
                }
            }
        }
    }

    pub(crate) fn locked(&self) -> bool {
        !self.locks.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, id: i64) -> bool {
        self.locks.contains(&id)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.locks.len()
    }
}
