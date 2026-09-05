use super::QueryEngine;

#[derive(Debug, Clone)]
pub(super) struct TempTableState {
    pub(super) committed: bool,
    pub(super) drop_on_commit: bool,
}

#[derive(Debug, Clone)]
pub(in crate::frontend) enum TempTableChange {
    Create { name: String, drop_on_commit: bool },
    Drop(String),
}

impl QueryEngine {
    /// Record a successful DISCARD TEMP.
    pub(super) fn discard_temp_tables(&mut self, in_transaction: bool) {
        if in_transaction {
            let discarded = self.discarded_temp_tables.get_or_insert_default();
            for (name, state) in std::mem::take(&mut self.temp_tables) {
                discarded.entry(name).or_insert(state);
            }
        } else {
            self.temp_tables.clear();
        }

        self.check_lock();
    }

    /// Commit or roll back temporary-table tracking changes.
    pub(super) fn finish_temp_table_transaction(&mut self, rollback: bool) {
        if rollback {
            if let Some(discarded) = self.discarded_temp_tables.take() {
                self.temp_tables.extend(discarded);
            }
            self.temp_tables.retain(|_, state| state.committed);
        } else {
            self.discarded_temp_tables = None;
            self.temp_tables.retain(|_, state| {
                state.committed = true;
                !state.drop_on_commit
            });
        }
    }
}
