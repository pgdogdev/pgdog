//! Keep track of server stats.

use std::time::Instant;

use fnv::FnvHashMap as HashMap;
use once_cell::sync::Lazy;
use parking_lot::Mutex;

use crate::{net::messages::BackendKeyData, state::State};

use super::pool::Address;

static STATS: Lazy<Mutex<HashMap<BackendKeyData, ConnectedServer>>> =
    Lazy::new(|| Mutex::new(HashMap::default()));

/// Get a copy of latest stats.
pub fn stats() -> HashMap<BackendKeyData, ConnectedServer> {
    STATS.lock().clone()
}

/// Update stats to latest version.
fn update(id: BackendKeyData, stats: Stats) {
    let mut guard = STATS.lock();
    if let Some(entry) = guard.get_mut(&id) {
        entry.stats = stats;
    }
}

/// Server is disconnecting.
fn disconnect(id: &BackendKeyData) {
    STATS.lock().remove(id);
}

/// Connected server.
#[derive(Clone, Debug)]
pub struct ConnectedServer {
    pub stats: Stats,
    pub addr: Address,
}

/// Server statistics.
#[derive(Copy, Clone, Debug)]
pub struct Stats {
    id: BackendKeyData,
    /// Number of bytes sent.
    pub bytes_sent: usize,
    /// Number of bytes received.
    pub bytes_received: usize,
    pub transactions: usize,
    pub queries: usize,
    pub rollbacks: usize,
    pub errors: usize,
    pub prepared_statements: usize,
    pub healthchecks: usize,
    pub state: State,
    pub last_used: Instant,
    pub last_healthcheck: Option<Instant>,
    pub created_at: Instant,
}

impl Stats {
    /// Register new server with statistics.
    pub fn connect(id: BackendKeyData, addr: &Address) -> Self {
        let now = Instant::now();
        let stats = Stats {
            id,
            bytes_sent: 0,
            bytes_received: 0,
            transactions: 0,
            queries: 0,
            rollbacks: 0,
            errors: 0,
            prepared_statements: 0,
            healthchecks: 0,
            state: State::Idle,
            last_used: now,
            last_healthcheck: None,
            created_at: now,
        };

        STATS.lock().insert(
            id,
            ConnectedServer {
                stats,
                addr: addr.clone(),
            },
        );

        stats
    }

    /// A transaction has been completed.
    pub fn transaction(&mut self) {
        self.transactions += 1;
        self.state = State::Idle;
        self.last_used = Instant::now();
        self.update();
    }

    /// Error occured in a transaction.
    pub fn transaction_error(&mut self) {
        self.transactions += 1;
        self.state = State::TransactionError;
        self.update();
    }

    /// An error occurred in general.
    pub fn error(&mut self) {
        self.errors += 1;
    }

    /// A query has been completed.
    pub fn query(&mut self) {
        self.queries += 1;
    }

    /// Manual state change.
    pub fn state(&mut self, state: State) {
        let update = self.state != state;
        self.state = state;
        if update {
            self.update();
        }
    }

    /// Send bytes to server.
    pub fn send(&mut self, bytes: usize) {
        self.bytes_sent += bytes;
    }

    /// Receive bytes from server.
    pub fn receive(&mut self, bytes: usize) {
        self.bytes_received += bytes;
    }

    /// Count prepared statements.
    pub fn prepared_statement(&mut self) {
        self.prepared_statements += 1;
        self.state = State::ParseComplete;
        self.update();
    }

    /// Track healtchecks.
    pub fn healthcheck(&mut self) {
        self.healthchecks += 1;
        self.last_healthcheck = Some(Instant::now());
        self.update();
    }

    /// Track rollbacks.
    pub fn rollback(&mut self) {
        self.rollbacks += 1;
        self.update();
    }

    /// Update server stats globally.
    pub fn update(&self) {
        update(self.id, *self)
    }

    /// Server is closing.
    pub(super) fn disconnect(&self) {
        disconnect(&self.id);
    }
}
