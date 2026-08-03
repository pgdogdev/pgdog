use std::ops::DerefMut;
use std::{ops::Deref, sync::Arc, time::SystemTime};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use pgdog_stats::{Lsn, SchemaStatementTask, TableCopyState};

use crate::backend::replication::ee::{
    data_sync_done, data_sync_error, data_sync_progress, replication_slot_create,
    replication_slot_drop, replication_slot_error, replication_slot_update,
};
use crate::backend::{pool::Address, replication::logical::Error as LogicalError};
use crate::net::ErrorResponse;

/// Status of table copies.
static COPIES: Lazy<TableCopies> = Lazy::new(TableCopies::default);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TableCopy {
    pub(crate) schema: Arc<String>,
    pub(crate) table: Arc<String>,
}

impl From<&TableCopy> for pgdog_stats::TableCopy {
    fn from(value: &TableCopy) -> Self {
        pgdog_stats::TableCopy {
            schema: value.schema.to_string(),
            table: value.table.to_string(),
        }
    }
}

impl TableCopy {
    pub(crate) fn new(schema: &str, table: &str) -> Self {
        let copy = Self {
            schema: Arc::new(schema.to_owned()),
            table: Arc::new(table.to_owned()),
        };
        let state = TableCopyState {
            last_update: SystemTime::now(),
            ..Default::default()
        };

        TableCopies::get().insert(copy.clone(), state.clone());

        data_sync_progress(&copy, &state);

        copy
    }

    pub(crate) fn update_progress(&self, bytes: usize, rows: usize) {
        if let Some(mut state) = TableCopies::get().get_mut(self) {
            state.bytes += bytes;
            state.rows += rows;
            let elapsed = SystemTime::now()
                .duration_since(state.last_update)
                .unwrap_or_default()
                .as_secs();
            if elapsed > 0 {
                state.bytes_per_sec = state.bytes / elapsed as usize;
            }

            data_sync_progress(self, &state);
        }
    }

    pub(crate) fn error(&self, error: &LogicalError) {
        data_sync_error(self, error);
    }

    pub(crate) fn update_sql(&self, sql: &str) {
        if let Some(mut state) = TableCopies::get().get_mut(self) {
            state.sql = Arc::new(sql.to_owned());
        }
    }

    /// Reset byte and row counters before retrying a failed table copy.
    /// Prevents accumulated counts from a discarded attempt inflating totals
    /// and throughput calculations across retries.
    pub(crate) fn reset(&self) {
        if let Some(mut state) = TableCopies::get().get_mut(self) {
            state.bytes = 0;
            state.rows = 0;
            state.bytes_per_sec = 0;
            state.last_update = SystemTime::now();
            data_sync_progress(self, &state);
        }
    }
}

impl Drop for TableCopy {
    fn drop(&mut self) {
        data_sync_done(self);
        COPIES.copies.remove(self);
    }
}

#[derive(Default, Clone)]
pub(crate) struct TableCopies {
    copies: Arc<DashMap<TableCopy, TableCopyState>>,
}

impl Deref for TableCopies {
    type Target = DashMap<TableCopy, TableCopyState>;

    fn deref(&self) -> &Self::Target {
        &self.copies
    }
}

impl TableCopies {
    pub(crate) fn get() -> Self {
        COPIES.clone()
    }
}

static REPLICATION_SLOTS: Lazy<ReplicationSlots> = Lazy::new(ReplicationSlots::default);

/// Replication slot.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationSlot {
    inner: pgdog_stats::ReplicationSlot,
}

impl Deref for ReplicationSlot {
    type Target = pgdog_stats::ReplicationSlot;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for ReplicationSlot {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl ReplicationSlot {
    pub(crate) fn new(name: &str, lsn: &Lsn, copy_data: bool, address: &Address) -> Self {
        let slot = Self {
            inner: pgdog_stats::ReplicationSlot {
                name: name.to_owned(),
                lsn: *lsn,
                copy_data,
                lag: 0,
                address: address.clone().into(),
                last_transaction: None,
            },
        };

        ReplicationSlots::get().insert(name.to_owned(), slot.clone());

        replication_slot_create(&slot.inner);

        slot
    }

    pub(crate) fn update_lsn(&self, lsn: &Lsn) {
        if let Some(mut slot) = ReplicationSlots::get().get_mut(&self.name) {
            slot.lsn = *lsn;
            slot.last_transaction = Some(SystemTime::now());
            replication_slot_update(&slot.inner);
        }
    }

    pub(crate) fn update_lag(&self, lag: i64) {
        if let Some(mut slot) = ReplicationSlots::get().get_mut(&self.name) {
            slot.lag = lag;
            replication_slot_update(&slot.inner);
        }
    }

    pub(crate) fn dropped(&self) {
        ReplicationSlots::get().remove(&self.name);
        replication_slot_drop(&self.inner);
    }

    pub(crate) fn error(&self, error: &ErrorResponse) {
        replication_slot_error(&self.inner, error);
    }
}

impl Drop for ReplicationSlot {
    fn drop(&mut self) {
        // The slot is dropped automatically by the connection,
        // and we don't call fn dropped manually, so we need to do that here
        // to track the slot is gone.
        if self.copy_data {
            self.dropped();
        }
    }
}

#[derive(Default, Clone, Debug)]
pub(crate) struct ReplicationSlots {
    slots: Arc<DashMap<String, ReplicationSlot>>,
}

impl ReplicationSlots {
    pub(crate) fn get() -> Self {
        REPLICATION_SLOTS.clone()
    }
}

impl Deref for ReplicationSlots {
    type Target = Arc<DashMap<String, ReplicationSlot>>;

    fn deref(&self) -> &Self::Target {
        &self.slots
    }
}

/// Legacy schema-sync push path. Schema-sync progress is now reported by the
/// task registry (`SHOW SCHEMA_SYNC`), so nothing is ever tracked here and the
/// snapshot is always empty. Kept so the control client keeps compiling
/// unchanged.
#[derive(Default, Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct SchemaStatements;

#[allow(dead_code)]
impl SchemaStatements {
    pub(crate) fn get() -> Self {
        Self
    }

    pub(crate) fn snapshot_and_clean(&self) -> Vec<SchemaStatementTask> {
        vec![]
    }
}
