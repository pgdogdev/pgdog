//! Keep track of server stats.

use std::{
    ops::Add,
    time::{Duration, SystemTime},
};

use fnv::FnvHashMap as HashMap;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::time::Instant;

use crate::{
    net::{messages::BackendKeyData, Parameters},
    state::State,
};

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
    pub application_name: String,
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
    pub query_time: Duration,
    pub transaction_time: Duration,
    pub parse: usize,
    pub bind: usize,
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
            query_time: self.query_time.saturating_add(rhs.query_time),
            transaction_time: self.query_time.saturating_add(rhs.transaction_time),
            parse: self.parse.saturating_add(rhs.parse),
            bind: self.bind.saturating_add(rhs.bind),
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
    query_timer: Option<Instant>,
    transaction_timer: Option<Instant>,
}

impl Stats {
    /// Register new server with statistics.
    pub fn connect(id: BackendKeyData, addr: &Address, params: &Parameters) -> Self {
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
            query_timer: None,
            transaction_timer: None,
        };

        STATS.lock().insert(
            id,
            ConnectedServer {
                stats,
                addr: addr.clone(),
                application_name: params.get_default("application_name", "PgDog").to_owned(),
            },
        );

        stats
    }

    fn transaction_state(&mut self, now: Instant, state: State) {
        self.total.transactions += 1;
        self.last_checkout.transactions += 1;
        self.state = state;
        self.last_used = now;
        if let Some(transaction_timer) = self.transaction_timer.take() {
            let duration = now.duration_since(transaction_timer);
            self.total.transaction_time += duration;
            self.last_checkout.transaction_time += duration;
        }
        self.update();
    }

    pub fn link_client(&mut self, client: &str, server: &str) {
        if client != server {
            let mut guard = STATS.lock();
            if let Some(entry) = guard.get_mut(&self.id) {
                entry.application_name.clear();
                entry.application_name.push_str(client);
            }
        }
    }

    pub fn parse_complete(&mut self) {
        self.total.parse += 1;
        self.last_checkout.parse += 1;
    }

    pub fn copy_mode(&mut self) {
        self.state(State::CopyMode);
    }

    pub fn bind_complete(&mut self) {
        self.total.bind += 1;
        self.last_checkout.bind += 1;
    }

    /// A transaction has been completed.
    pub fn transaction(&mut self, now: Instant) {
        self.transaction_state(now, State::Idle);
    }

    /// Error occurred in a transaction.
    pub fn transaction_error(&mut self, now: Instant) {
        self.transaction_state(now, State::TransactionError);
    }

    /// An error occurred in general.
    pub fn error(&mut self) {
        self.total.errors += 1;
        self.last_checkout.errors += 1;
    }

    /// A query has been completed.
    pub fn query(&mut self, now: Instant) {
        self.total.queries += 1;
        self.last_checkout.queries += 1;
        if let Some(query_timer) = self.query_timer.take() {
            let duration = now.duration_since(query_timer);
            self.total.query_time += duration;
            self.last_checkout.query_time += duration;
        }
    }

    pub(crate) fn set_timers(&mut self, now: Instant) {
        self.transaction_timer = Some(now);
        self.query_timer = Some(now);
    }

    /// Manual state change.
    pub fn state(&mut self, state: State) {
        let update = self.state != state;
        self.state = state;
        if update {
            self.activate();
            self.update();
        }
    }

    fn activate(&mut self) {
        if self.state == State::Active {
            let now = Instant::now();
            if self.transaction_timer.is_none() {
                self.transaction_timer = Some(now);
            }
            if self.query_timer.is_none() {
                self.query_timer = Some(now);
            }
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
