use std::ops::DerefMut;
use std::{ops::Deref, sync::Arc, time::SystemTime};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use pgdog_stats::{Lsn, SchemaStatementTask};

use crate::backend::pool::Address;
use crate::backend::replication::ee::{
    replication_slot_create, replication_slot_drop, replication_slot_error, replication_slot_update,
};
use crate::net::ErrorResponse;

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
