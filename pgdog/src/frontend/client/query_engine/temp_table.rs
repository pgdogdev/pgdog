use fnv::FnvHashMap;

#[derive(Debug, Clone)]
struct TempTableState {
    committed: bool,
    drop_on_commit: bool,
}

#[derive(Debug, Clone)]
pub(in crate::frontend) enum TempTableChange {
    Create { name: String, drop_on_commit: bool },
    Drop(String),
}

#[derive(Debug, Default)]
pub(super) struct TempTables {
    tables: FnvHashMap<String, TempTableState>,
    /// Tables removed by DISCARD TEMP in the current transaction.
    /// Keep them until COMMIT so ROLLBACK can restore the client-side tracker.
    discarded: Option<FnvHashMap<String, TempTableState>>,
}

impl TempTables {
    pub(super) fn update(&mut self, change: &TempTableChange, in_transaction: bool) {
        match change {
            TempTableChange::Create {
                name,
                drop_on_commit,
            } => {
                self.tables.insert(
                    name.clone(),
                    TempTableState {
                        committed: !in_transaction,
                        drop_on_commit: *drop_on_commit,
                    },
                );
            }
            TempTableChange::Drop(table) => {
                self.tables.remove(table);
            }
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.tables.is_empty() && self.discarded.as_ref().is_none_or(FnvHashMap::is_empty)
    }

    /// Record a successful DISCARD TEMP.
    pub(super) fn discard(&mut self, in_transaction: bool) {
        if in_transaction {
            let discarded = self.discarded.get_or_insert_default();
            for (name, state) in std::mem::take(&mut self.tables) {
                discarded.entry(name).or_insert(state);
            }
        } else {
            self.tables.clear();
        }
    }

    /// Commit or roll back temporary-table tracking changes.
    pub(super) fn finish_transaction(&mut self, rollback: bool) {
        if rollback {
            if let Some(discarded) = self.discarded.take() {
                self.tables.extend(discarded);
            }
            self.tables.retain(|_, state| state.committed);
        } else {
            self.discarded = None;
            self.tables.retain(|_, state| {
                state.committed = true;
                !state.drop_on_commit
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(name: &str) -> TempTableChange {
        TempTableChange::Create {
            name: name.to_string(),
            drop_on_commit: false,
        }
    }

    #[test]
    fn discard_rollback_restores_committed_tables() {
        let mut tables = TempTables::default();
        tables.update(&create("foo"), false);

        tables.discard(true);
        tables.finish_transaction(true);

        assert!(!tables.is_empty());
    }

    #[test]
    fn discard_rollback_removes_uncommitted_tables() {
        let mut tables = TempTables::default();
        tables.update(&create("foo"), true);

        tables.discard(true);
        tables.finish_transaction(true);

        assert!(tables.is_empty());
    }

    #[test]
    fn discard_commit_removes_tables() {
        let mut tables = TempTables::default();
        tables.update(&create("foo"), false);

        tables.discard(true);
        tables.finish_transaction(false);

        assert!(tables.is_empty());
    }
}
