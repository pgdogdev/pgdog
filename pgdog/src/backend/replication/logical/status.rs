use std::ops::DerefMut;
use std::{ops::Deref, sync::Arc, time::SystemTime};

use dashmap::{DashMap, DashSet};
use once_cell::sync::Lazy;
use pgdog_stats::{Lsn, StatementKind, TableCopyState};

use crate::backend::{
    pool::Address,
    schema::sync::{Statement, SyncState},
    Cluster,
};

/// Status of table copies.
static COPIES: Lazy<TableCopies> = Lazy::new(TableCopies::default);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableCopy {
    pub(crate) schema: String,
    pub(crate) table: String,
}

impl From<&TableCopy> for pgdog_stats::TableCopy {
    fn from(value: &TableCopy) -> Self {
        pgdog_stats::TableCopy {
            schema: value.schema.clone(),
            table: value.table.clone(),
        }
    }
}

impl TableCopy {
    pub(crate) fn new(schema: &str, table: &str) -> Self {
        let copy = Self {
            schema: schema.to_owned(),
            table: table.to_owned(),
        };
        TableCopies::get().insert(
            copy.clone(),
            TableCopyState {
                last_update: SystemTime::now(),
                ..Default::default()
            },
        );
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
        }
    }

    pub(crate) fn update_sql(&self, sql: &str) {
        if let Some(mut state) = TableCopies::get().get_mut(self) {
            state.sql = sql.to_owned();
        }
    }
}

impl Drop for TableCopy {
    fn drop(&mut self) {
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

        slot
    }

    pub(crate) fn update_lsn(&self, lsn: &Lsn) {
        if let Some(mut slot) = ReplicationSlots::get().get_mut(&self.name) {
            slot.lsn = *lsn;
            slot.last_transaction = Some(SystemTime::now());
        }
    }

    pub(crate) fn update_lag(&self, lag: i64) {
        if let Some(mut slot) = ReplicationSlots::get().get_mut(&self.name) {
            slot.lag = lag;
        }
    }

    pub(crate) fn dropped(&self) {
        ReplicationSlots::get().remove(&self.name);
    }
}

impl Drop for ReplicationSlot {
    fn drop(&mut self) {
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
    inner: pgdog_stats::SchemaStatement,
}

impl From<pgdog_stats::SchemaStatement> for SchemaStatement {
    fn from(value: pgdog_stats::SchemaStatement) -> Self {
        Self { inner: value }
    }
}

impl Deref for SchemaStatement {
    type Target = pgdog_stats::SchemaStatement;

    fn deref(&self) -> &Self::Target {
        &self.inner
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

        let stmt: Self = match stmt {
            Statement::Index { table, sql, .. } => pgdog_stats::SchemaStatement {
                user: user.into(),
                shard,
                sql: sql.clone(),
                kind: StatementKind::Index,
                sync_state,
                started_at: SystemTime::now(),
                table_schema: table.schema.map(|s| s.to_string()),
                table_name: Some(table.name.to_owned()),
            },
            Statement::Table { table, sql } => pgdog_stats::SchemaStatement {
                user: user.into(),
                shard,
                sql: sql.clone(),
                kind: StatementKind::Table,
                sync_state,
                started_at: SystemTime::now(),
                table_schema: table.schema.map(|s| s.to_string()),
                table_name: Some(table.name.to_owned()),
            },
            Statement::Other { sql, .. } => pgdog_stats::SchemaStatement {
                user: user.into(),
                shard,
                sql: sql.clone(),
                kind: StatementKind::Statement,
                sync_state,
                started_at: SystemTime::now(),
                table_schema: None,
                table_name: None,
            },
            Statement::SequenceOwner { sql, .. } => pgdog_stats::SchemaStatement {
                user: user.into(),
                shard,
                sql: sql.to_string(),
                kind: StatementKind::Statement,
                sync_state,
                started_at: SystemTime::now(),
                table_schema: None,
                table_name: None,
            },
            Statement::SequenceSetMax { sql, .. } => pgdog_stats::SchemaStatement {
                user: user.into(),
                shard,
                sql: sql.clone(),
                kind: StatementKind::Statement,
                sync_state,
                started_at: SystemTime::now(),
                table_schema: None,
                table_name: None,
            },
        }
        .into();

        SchemaStatements::get().insert(stmt.clone());

        stmt
    }
}

impl Drop for SchemaStatement {
    fn drop(&mut self) {
        SchemaStatements::get().remove(self);
    }
}

#[derive(Default, Debug, Clone)]
pub struct SchemaStatements {
    stmts: Arc<DashSet<SchemaStatement>>,
}

impl SchemaStatements {
    pub(crate) fn get() -> Self {
        SCHEMA_STATEMENTS.clone()
    }
}

impl Deref for SchemaStatements {
    type Target = Arc<DashSet<SchemaStatement>>;

    fn deref(&self) -> &Self::Target {
        &self.stmts
    }
}

static SCHEMA_STATEMENTS: Lazy<SchemaStatements> = Lazy::new(SchemaStatements::default);
