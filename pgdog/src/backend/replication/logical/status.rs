use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use once_cell::sync::Lazy;
use parking_lot::Mutex;

use crate::backend::{databases::User, replication::publisher::Table, Cluster};

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
