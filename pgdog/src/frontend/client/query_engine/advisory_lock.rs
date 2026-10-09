use fnv::FnvHashSet;

use crate::{
    frontend::router::parser::statement::{
        AdvisoryLockId, AdvisoryLocks as ParserAdvisoryLocks, LockScope,
    },
    net::{DataRow, Error, FromBytes, Message, ToBytes},
};

/// Tracks advisory locks held by the current client across requests.
#[derive(Default, Debug)]
pub(crate) struct AdvisoryLocks {
    locks: FnvHashSet<AdvisoryLockId>,
    /// pg_try_advisory_lock returned false.
    not_acquired: bool,
}

impl AdvisoryLocks {
    /// Check if pg_try_advisory_lock acquired the lock.
    pub(crate) fn data_row(
        &mut self,
        locks: &ParserAdvisoryLocks,
        message: &Message,
    ) -> Result<(), Error> {
        if locks.try_lock() {
            let row = DataRow::from_bytes(message.to_bytes())?;
            // Text format is 't' / 'f', binary is 1 / 0.
            self.not_acquired = !matches!(row.column(0).as_deref(), Some(b"t" | b"\x01"));
        }

        Ok(())
    }

    pub(crate) fn merge(&mut self, locks: &ParserAdvisoryLocks) {
        let not_acquired = std::mem::take(&mut self.not_acquired) && locks.try_lock();

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
                && !not_acquired
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
