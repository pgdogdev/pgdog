use fnv::FnvHashMap;

use crate::frontend::router::parser::advisory_lock::{AdvisoryLock, AdvisoryLockId, LockScope};

/// Tracks advisory locks held by the current client across requests.
#[derive(Default, Debug)]
pub(crate) struct AdvisoryLocks {
    locks: FnvHashMap<AdvisoryLockId, AdvisoryLock>,
}

impl AdvisoryLocks {
    pub(crate) fn merge<'a>(&mut self, locks: impl IntoIterator<Item = &'a AdvisoryLock>) {
        for lock in locks {
            if lock.unlock_all {
                self.locks.clear();
            } else if lock.unlock {
                // An unresolved individual unlock cannot release every tracked lock.
                if let Some(id) = lock.id {
                    self.locks.remove(&id);
                }
            } else if let Some(id) = lock.id
                && lock.scope == LockScope::Session
                && !self.locks.contains_key(&id)
            {
                self.locks.insert(id, *lock);
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
        self.locks.contains_key(&id)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.locks.len()
    }
}
