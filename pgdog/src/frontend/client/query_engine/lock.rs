use super::*;
use crate::net::{DataRow, Protocol};

impl QueryEngine {
    /// Check if we need to lock the backend to this client, and do so
    /// if needed.
    pub(super) fn sync_lock(&mut self) {
        // The presence of advisory locks or manual pin
        // indicates we cannot release the backend.
        let locked =
            self.advisory_locks.locked() || !self.temp_tables.is_empty() || self.manual_lock;

        self.backend.lock(locked);
        self.stats.locked(locked);
    }

    pub(super) fn handle_advisory_locks(&mut self, message: &Message) {
        let locks = self.router.command().route().advisory_locks();
        let row = if message.code() == 'D'
            && !message.streaming()
            && locks
                .iter()
                .any(|lock| lock.try_lock && lock.column_index.is_some())
        {
            DataRow::try_from(message.clone()).ok()
        } else {
            None
        };
        self.advisory_locks.merge(locks.iter().filter(|lock| {
            // Parser couldn't extract column index.
            let Some(column_index) = lock.column_index.filter(|_| lock.try_lock) else {
                return true;
            };

            // A directly selected try-lock is handled only on its result row.
            // In particular, ReadyForQuery must not re-add a failed attempt.
            if message.code() != 'D' || lock.row_index != self.result_row_counter {
                return false;
            }

            match row.as_ref().and_then(|row| row.get_raw(column_index)) {
                Some(value) if value.is_null => false,
                // Booleans use t/f in text and 1/0 in binary. This also handles
                // Execute without Describe, where no RowDescription is sent.
                Some(value) => match value.data.as_ref() {
                    b"t" | [1] => true,
                    b"f" | [0] => false,
                    _ => true,
                },
                // Keep the backend pinned if a result cannot be inspected.
                None => true,
            }
        }));
        self.sync_lock();
    }
}
