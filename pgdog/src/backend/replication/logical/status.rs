use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    sync::Arc,
    time::SystemTime,
};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use parking_lot::Mutex;

use crate::backend::{databases::User, replication::publisher::Table, Cluster};

static COPIES: Lazy<TableCopies> = Lazy::new(|| TableCopies::default());

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableCopy {
    schema: String,
    table: String,
}

impl TableCopy {
    pub(crate) fn new(schema: &str, table: &str, sql: &str) -> Self {
        let copy = Self {
            schema: schema.to_owned(),
            table: table.to_owned(),
        };
        TableCopies::get().insert(
            copy.clone(),
            TableCopyState {
                sql: sql.to_owned(),
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
                .as_secs_f64();
            if elapsed > 0.0 {
                state.bytes_per_sec = state.bytes / elapsed as usize;
            }
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

static STATE: Lazy<Mutex<HashMap<usize, ReplicationState>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static REPLICATION: Lazy<Mutex<Option<ReplicationClusters>>> = Lazy::new(|| Mutex::new(None));

fn update_or_insert(tracker: &ReplicationTracker, state: State) {
    let mut guard = STATE.lock();
    if let Some(entry) = guard.get_mut(&tracker.shard) {
        entry.state = state;
    } else {
        guard.insert(
            tracker.shard,
            ReplicationState {
                state,
                tables: HashSet::new(),
            },
        );
    }
}

#[derive(Default, Clone)]
pub struct ReplicationClusters {
    pub src: Arc<User>,
    pub dest: Arc<User>,
}

impl ReplicationClusters {
    pub fn start(src: &Cluster, dest: &Cluster) {
        *REPLICATION.lock() = Some(ReplicationClusters {
            src: src.identifier(),
            dest: dest.identifier(),
        });
    }

    pub fn end() {
        *REPLICATION.lock() = None;
        STATE.lock().clear();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ReplicationTracker {
    shard: usize,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum State {
    #[default]
    SchemaDump,
    SchemaRestore {
        stmt: usize,
        total: usize,
    },
    CopyData {
        table: usize,
        total: usize,
    },
    CreateSlot,
    Replication {
        lag: i64,
    },
}

#[derive(Default, Clone)]
pub struct ReplicationState {
    pub state: State,
    pub tables: HashSet<Table>,
}

pub fn resharding_status() -> (
    Option<ReplicationClusters>,
    HashMap<usize, ReplicationState>,
) {
    let replication = REPLICATION.lock().clone();
    let state = STATE.lock().clone();
    (replication, state)
}

impl ReplicationTracker {
    pub(crate) fn start(src: &Cluster, dest: &Cluster) {
        ReplicationClusters::start(src, dest);
    }

    pub(crate) fn new(shard: usize) -> Self {
        Self { shard }
    }

    pub(crate) fn schema_dump(&self) {
        update_or_insert(self, State::SchemaDump);
    }

    pub(crate) fn schema_restore(&self, stmt: usize, total: usize) {
        update_or_insert(self, State::SchemaRestore { stmt, total });
    }

    pub(crate) fn replication_lag(&self, lag: i64) {
        update_or_insert(self, State::Replication { lag });
    }

    pub(crate) fn create_slot(&self) {
        update_or_insert(self, State::CreateSlot);
    }

    pub(crate) fn copy_data(&self, table: &Table) {
        STATE
            .lock()
            .entry(self.shard)
            .or_insert_with(ReplicationState::default)
            .tables
            .insert(table.clone());
    }

    pub(crate) fn copy_data_done(&self, table: &Table) {
        STATE
            .lock()
            .entry(self.shard)
            .or_insert_with(ReplicationState::default)
            .tables
            .remove(table);
    }
}
