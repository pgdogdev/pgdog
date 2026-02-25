use std::{ops::Deref, sync::Arc, time::SystemTime};

use dashmap::{DashMap, DashSet};
use once_cell::sync::Lazy;
use pgdog_stats::Lsn;

use crate::backend::{
    databases::User,
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

#[derive(Debug, Clone)]
pub struct TableCopyState {
    pub(crate) sql: String,
    pub(crate) rows: usize,
    pub(crate) bytes: usize,
    pub(crate) bytes_per_sec: usize,
    pub(crate) last_update: SystemTime,
}

impl Default for TableCopyState {
    fn default() -> Self {
        Self {
            sql: String::default(),
            rows: 0,
            bytes: 0,
            bytes_per_sec: 0,
            last_update: SystemTime::now(),
        }
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
    pub(crate) name: String,
    pub(crate) lsn: Lsn,
    pub(crate) lag: i64,
    pub(crate) copy_data: bool,
    pub(crate) address: Address,
    pub(crate) last_transaction: Option<SystemTime>,
}

impl ReplicationSlot {
    pub(crate) fn new(name: &str, lsn: &Lsn, copy_data: bool, address: &Address) -> Self {
        let slot = Self {
            name: name.to_owned(),
            lsn: *lsn,
            copy_data,
            lag: 0,
            address: address.clone(),
            last_transaction: None,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StatementKind {
    Table,
    Index,
    Statement,
}

impl std::fmt::Display for StatementKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Table => write!(f, "table"),
            Self::Index => write!(f, "index"),
            Self::Statement => write!(f, "statement"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct SchemaStatement {
    pub(crate) user: User,
    pub(crate) shard: usize,
    pub(crate) sql: String,
    pub(crate) kind: StatementKind,
    pub(crate) sync_state: SyncState,
    pub(crate) started_at: SystemTime,
    pub(crate) table_schema: Option<String>,
    pub(crate) table_name: Option<String>,
}

impl SchemaStatement {
    pub(crate) fn new(
        cluster: &Cluster,
        stmt: &Statement<'_>,
        shard: usize,
        sync_state: SyncState,
    ) -> Self {
        let user = cluster.identifier().deref().clone();

        let stmt = match stmt {
            Statement::Index { table, sql, .. } => Self {
                user,
                shard,
                sql: sql.clone(),
                kind: StatementKind::Index,
                sync_state,
                started_at: SystemTime::now(),
                table_schema: table.schema.map(|s| s.to_string()),
                table_name: Some(table.name.to_owned()),
            },
            Statement::Table { table, sql } => Self {
                user,
                shard,
                sql: sql.clone(),
                kind: StatementKind::Table,
                sync_state,
                started_at: SystemTime::now(),
                table_schema: table.schema.map(|s| s.to_string()),
                table_name: Some(table.name.to_owned()),
            },
            Statement::Other { sql, .. } => Self {
                user,
                shard,
                sql: sql.clone(),
                kind: StatementKind::Statement,
                sync_state,
                started_at: SystemTime::now(),
                table_schema: None,
                table_name: None,
            },
            Statement::SequenceOwner { sql, .. } => Self {
                user,
                shard,
                sql: sql.to_string(),
                kind: StatementKind::Statement,
                sync_state,
                started_at: SystemTime::now(),
                table_schema: None,
                table_name: None,
            },
            Statement::SequenceSetMax { sql, .. } => Self {
                user,
                shard,
                sql: sql.clone(),
                kind: StatementKind::Statement,
                sync_state,
                started_at: SystemTime::now(),
                table_schema: None,
                table_name: None,
            },
        };

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
