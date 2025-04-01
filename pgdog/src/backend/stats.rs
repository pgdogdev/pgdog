//! Keep track of server stats.

use std::{
    ops::Add,
    time::{Instant, SystemTime},
};

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

/// Server connection stats.
#[derive(Copy, Clone, Debug, Default)]
pub struct Counts {
    pub bytes_sent: usize,
    pub bytes_received: usize,
    pub transactions: usize,
    pub queries: usize,
    pub rollbacks: usize,
    pub errors: usize,
    pub prepared_statements: usize,
}

impl Add for Counts {
    type Output = Counts;

    fn add(self, rhs: Self) -> Self::Output {
        Counts {
            bytes_sent: self.bytes_sent.saturating_add(rhs.bytes_sent),
            bytes_received: self.bytes_received.saturating_add(rhs.bytes_received),
            transactions: self.transactions.saturating_add(rhs.transactions),
            queries: self.queries.saturating_add(rhs.queries),
            rollbacks: self.rollbacks.saturating_add(rhs.rollbacks),
            errors: self.errors.saturating_add(rhs.errors),
            prepared_statements: self
                .prepared_statements
                .saturating_add(rhs.prepared_statements),
        }
    }
}

/// Server statistics.
#[derive(Copy, Clone, Debug)]
pub struct Stats {
    pub id: BackendKeyData,
    /// Number of bytes sent.
    pub healthchecks: usize,
    pub state: State,
    pub last_used: Instant,
    pub last_healthcheck: Option<Instant>,
    pub created_at: Instant,
    pub created_at_time: SystemTime,
    pub total: Counts,
    pub last_checkout: Counts,
}

impl Stats {
    /// Register new server with statistics.
    pub fn connect(id: BackendKeyData, addr: &Address) -> Self {
        let now = Instant::now();
        let stats = Stats {
            id,
            healthchecks: 0,
            state: State::Idle,
            last_used: now,
            last_healthcheck: None,
            created_at: now,
            created_at_time: SystemTime::now(),
            total: Counts::default(),
            last_checkout: Counts::default(),
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
        self.total.transactions += 1;
        self.last_checkout.transactions += 1;
        self.state = State::Idle;
        self.last_used = Instant::now();
        self.update();
    }

    /// Error occurred in a transaction.
    pub fn transaction_error(&mut self) {
        self.total.transactions += 1;
        self.last_checkout.transactions += 1;
        self.state = State::TransactionError;
        self.update();
    }

    /// An error occurred in general.
    pub fn error(&mut self) {
        self.total.errors += 1;
        self.last_checkout.errors += 1;
    }

    /// A query has been completed.
    pub fn query(&mut self) {
        self.total.queries += 1;
        self.last_checkout.queries += 1;
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
        self.total.bytes_sent += bytes;
        self.last_checkout.bytes_sent += bytes;
    }

    /// Receive bytes from server.
    pub fn receive(&mut self, bytes: usize) {
        self.total.bytes_received += bytes;
        self.last_checkout.bytes_received += bytes;
    }

    /// Count prepared statements.
    pub fn prepared_statement(&mut self) {
        self.total.prepared_statements += 1;
        self.last_checkout.prepared_statements += 1;
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
        self.total.rollbacks += 1;
        self.last_checkout.rollbacks += 1;
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

    /// Reset last_checkout counts.
    pub fn reset_last_checkout(&mut self) -> Counts {
        let counts = self.last_checkout;
        self.last_checkout = Counts::default();
        counts
    }
}
