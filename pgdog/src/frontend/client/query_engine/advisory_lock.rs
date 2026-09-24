use fnv::FnvHashSet;

use crate::frontend::router::parser::statement::{
    AdvisoryLockId, AdvisoryLocks as ParserAdvisoryLocks, LockScope,
};

/// Tracks advisory locks held by the current client across requests.
#[derive(Default, Debug)]
pub(crate) struct AdvisoryLocks {
    locks: FnvHashSet<AdvisoryLockId>,
}

impl AdvisoryLocks {
    pub(crate) fn merge(&mut self, locks: &ParserAdvisoryLocks) {
        for lock in locks.iter() {
            if lock.unlock_all {
                self.locks.clear();
            } else if lock.unlock {
                // An unresolved individual unlock cannot release every tracked lock.
                if let Some(id) = lock.id {
                    self.locks.remove(&id);
                }
            } else if let Some(id) = lock.id
                && lock.scope == LockScope::Session
            {
                self.locks.insert(id);
            }
        }
    }

    pub(crate) fn locked(&self) -> bool {
        !self.locks.is_empty()
    }

    pub(crate) fn clear(&mut self) {
        self.locks.clear();
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, id: AdvisoryLockId) -> bool {
        self.locks.contains(&id)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.locks.len()
    }
}
