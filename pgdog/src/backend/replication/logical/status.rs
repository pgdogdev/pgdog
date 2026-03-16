use std::ops::DerefMut;
use std::sync::atomic::{AtomicI64, Ordering};
use std::{ops::Deref, sync::Arc, time::SystemTime};

use dashmap::{DashMap, DashSet};
use once_cell::sync::Lazy;
use pgdog_stats::{Lsn, SchemaStatementTask, StatementKind, TableCopyState};

use crate::backend::replication::ee::{
    data_sync_done, data_sync_error, data_sync_progress, replication_slot_create,
    replication_slot_drop, replication_slot_error, replication_slot_update, schema_sync_task,
};
use crate::backend::{
    pool::Address,
    replication::logical::Error as LogicalError,
    schema::sync::{Statement, SyncState},
    Cluster,
};
use crate::net::ErrorResponse;

/// Status of table copies.
static COPIES: Lazy<TableCopies> = Lazy::new(TableCopies::default);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableCopy {
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
}

impl Drop for TableCopy {
    fn drop(&mut self) {
        data_sync_done(self);
        COPIES.copies.remove(self);
    }
}

#[derive(Default, Clone)]
pub struct TableCopies {
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
pub struct ReplicationSlot {
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
pub struct ReplicationSlots {
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

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct SchemaStatement {
    task: SchemaStatementTask,
}

impl Deref for SchemaStatement {
    type Target = pgdog_stats::SchemaStatement;

    fn deref(&self) -> &Self::Target {
        &self.task.statement
    }
}

impl SchemaStatement {
    pub(crate) fn new(
        cluster: &Cluster,
        stmt: &Statement<'_>,
        shard: usize,
        sync_state: SyncState,
    ) -> Self {
        let user = cluster.identifier().deref().clone();
        let id = SchemaStatements::next_id();

        let stmt = match stmt {
            Statement::Index { table, sql, .. } => pgdog_stats::SchemaStatement {
                id,
                user: user.into(),
                shard,
                sql: sql.clone(),
                kind: StatementKind::Index,
                sync_state,
                started_at: None,
                table_schema: table.schema.map(|s| s.to_string()),
                table_name: Some(table.name.to_owned()),
            },
            Statement::Table { table, sql } => pgdog_stats::SchemaStatement {
                id,
                user: user.into(),
                shard,
                sql: sql.clone(),
                kind: StatementKind::Table,
                sync_state,
                started_at: None,
                table_schema: table.schema.map(|s| s.to_string()),
                table_name: Some(table.name.to_owned()),
            },
            Statement::Other { sql, .. } => pgdog_stats::SchemaStatement {
                id,
                user: user.into(),
                shard,
                sql: sql.clone(),
                kind: StatementKind::Statement,
                sync_state,
                started_at: None,
                table_schema: None,
                table_name: None,
            },
            Statement::SequenceOwner { sql, .. } => pgdog_stats::SchemaStatement {
                id,
                user: user.into(),
                shard,
                sql: sql.to_string(),
                kind: StatementKind::Statement,
                sync_state,
                started_at: None,
                table_schema: None,
                table_name: None,
            },
            Statement::SequenceSetMax { sql, .. } => pgdog_stats::SchemaStatement {
                id,
                user: user.into(),
                shard,
                sql: sql.clone(),
                kind: StatementKind::Statement,
                sync_state,
                started_at: None,
                table_schema: None,
                table_name: None,
            },
        };

        let task = SchemaStatementTask {
            statement: stmt,
            running: false,
            done: false,
            error: None,
        };

        SchemaStatements::get().insert(task.clone());

        schema_sync_task(&task);

        Self { task }
    }

    pub(crate) fn running(&mut self) {
        if let Some(entry) = SchemaStatements::get()
            .stmts
            .remove(&self.task)
            .map(|mut entry| {
                entry.running = true;
                entry.statement.started_at = Some(SystemTime::now());

                entry
            })
        {
            self.task = entry.clone();
            schema_sync_task(&self.task);
            SchemaStatements::get().insert(self.task.clone());
        }
    }

    pub(crate) fn error(&mut self, err: &ErrorResponse) {
        if let Some(mut entry) = SchemaStatements::get().stmts.remove(&self.task) {
            entry.error = Some(err.to_string());
            entry.done = true;
            self.task = entry.clone();
            schema_sync_task(&self.task);
            SchemaStatements::get().insert(self.task.clone());
        }
    }
}

impl Drop for SchemaStatement {
    fn drop(&mut self) {
        SchemaStatements::get().remove(&self.task);

        self.task.done = true;
        schema_sync_task(&self.task);
    }
}

#[derive(Default, Debug, Clone)]
pub struct SchemaStatements {
    stmts: Arc<DashSet<SchemaStatementTask>>,
    id: Arc<AtomicI64>,
}

impl SchemaStatements {
    pub(crate) fn get() -> Self {
        SCHEMA_STATEMENTS.clone()
    }

    pub(crate) fn next_id() -> i64 {
        Self::get().id.fetch_add(1, Ordering::SeqCst)
    }
}

impl Deref for SchemaStatements {
    type Target = Arc<DashSet<SchemaStatementTask>>;

    fn deref(&self) -> &Self::Target {
        &self.stmts
    }
}

static SCHEMA_STATEMENTS: Lazy<SchemaStatements> = Lazy::new(SchemaStatements::default);
