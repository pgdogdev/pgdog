use fnv::FnvHashSet;

use crate::{
    frontend::router::parser::statement::{AdvisoryLocks as ParserAdvisoryLocks, LockScope},
    net::{Bind, DataRow, Error, Format, FromBytes, Message, ToBytes},
};

/// Tracks advisory locks held by the current client across requests.
#[derive(Default, Debug)]
pub(crate) struct AdvisoryLocks {
    locks: FnvHashSet<i64>,
    successful_try_locks: FnvHashSet<i64>,
    indeterminate_try_locks: FnvHashSet<i64>,
    result_formats: Vec<Format>,
    result_row: usize,
}

impl AdvisoryLocks {
    pub(crate) fn bind(&mut self, bind: &Bind) {
        self.result_formats.clear();
        self.result_formats.extend(bind.result_formats());
    }

    pub(crate) fn simple_query(&mut self) {
        self.result_formats.clear();
    }

    pub(crate) fn query_error(&mut self, locks: &ParserAdvisoryLocks) {
        self.indeterminate_try_locks.extend(locks.try_lock_ids());
    }

    pub(crate) fn process_data_row(
        &mut self,
        locks: &ParserAdvisoryLocks,
        message: &Message,
    ) -> Result<(), Error> {
        if !locks.has_inspectable_try_locks() {
            return Ok(());
        }

        let row = DataRow::from_bytes(message.to_bytes())?;
        for (column, lock) in locks.try_locks(self.result_row) {
            let format = match self.result_formats.as_slice() {
                [format] => *format,
                formats => formats.get(column).copied().unwrap_or(Format::Text),
            };
            if let Some(id) = lock.id {
                match row.get::<bool>(column, format) {
                    Some(true) => {
                        self.successful_try_locks.insert(id);
                    }
                    Some(false) => {}
                    None => {
                        // If PostgreSQL changes the result shape, keep the
                        // optimistic pin rather than return a locked backend.
                        self.indeterminate_try_locks.insert(id);
                    }
                }
            }
        }
        self.result_row += 1;

        Ok(())
    }

    pub(crate) fn merge(&mut self, locks: &ParserAdvisoryLocks) {
        for lock in locks.iter() {
            if lock.unlock {
                if let Some(id) = lock.id {
                    self.locks.remove(&id);
                } else {
                    // pg_advisory_unlock_all() clears every advisory lock.
                    self.locks.clear();
                }
            } else if let Some(id) = lock.id
                && lock.scope == LockScope::Session
                && (!locks.inspects_try_lock(lock)
                    || self.successful_try_locks.contains(&id)
                    || self.indeterminate_try_locks.contains(&id))
            {
                self.locks.insert(id);
            }
        }

        self.successful_try_locks.clear();
        self.indeterminate_try_locks.clear();
        self.result_formats.clear();
        self.result_row = 0;
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
