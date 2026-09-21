use fnv::FnvHashSet;

use crate::frontend::router::parser::statement::{
    AdvisoryLocks as ParserAdvisoryLocks, LockAction, LockScope,
};

/// Tracks advisory locks held by the current client across requests.
#[derive(Default, Debug)]
pub(crate) struct AdvisoryLocks {
    locks: FnvHashSet<i64>,
}

impl AdvisoryLocks {
    pub(crate) fn merge(&mut self, locks: &ParserAdvisoryLocks) {
        for lock in locks.iter() {
            match lock.action {
                LockAction::UnlockAll => self.locks.clear(),
                LockAction::Unlock if let Some(id) = lock.id => {
                    self.locks.remove(&id);
                }
                LockAction::Lock
                    if let Some(id) = lock.id
                        && lock.scope == LockScope::Session =>
                {
                    self.locks.insert(id);
                }
                // An individual unlock with an unknown or NULL key cannot
                // prove that any of the client's tracked locks were released.
                _ => {}
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
    pub(crate) fn contains(&self, id: i64) -> bool {
        self.locks.contains(&id)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.locks.len()
    }
}
