//! Keep track of server stats.

use std::ops::{Deref, DerefMut};

use fnv::FnvHashMap as HashMap;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
pub use pgdog_stats::server::Counts;
use tokio::time::Instant;

use crate::{
    backend::{pool::stats::MemoryStats, Pool, ServerOptions},
    config::Memory,
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

/// Get idle-in-transaction server connections for connection pool.
pub fn idle_in_transaction(pool: &Pool) -> usize {
    STATS
        .lock()
        .values()
        .filter(|stat| {
            stat.stats.pool_id == pool.id() && stat.stats.state == State::IdleInTransaction
        })
        .count()
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
    pub client: Option<BackendKeyData>,
}

/// Server statistics.
#[derive(Copy, Clone, Debug)]
pub struct Stats {
    inner: pgdog_stats::server::Stats,
    pub id: BackendKeyData,
    pub last_used: Instant,
    pub last_healthcheck: Option<Instant>,
    pub created_at: Instant,
    pub client_id: Option<BackendKeyData>,
    query_timer: Option<Instant>,
    transaction_timer: Option<Instant>,
    idle_in_transaction_timer: Option<Instant>,
}

impl Deref for Stats {
    type Target = pgdog_stats::server::Stats;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Stats {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Stats {
    /// Register new server with statistics.
    pub fn connect(
        id: BackendKeyData,
        addr: &Address,
        params: &Parameters,
        options: &ServerOptions,
        config: &Memory,
    ) -> Self {
        let now = Instant::now();
        let mut stats = Stats {
            inner: pgdog_stats::server::Stats::default(),
            id,
            last_used: now,
            last_healthcheck: None,
            created_at: now,
            query_timer: None,
            transaction_timer: None,
            client_id: None,
            idle_in_transaction_timer: None,
        };

        stats.inner.memory = *MemoryStats::new(config);
        stats.inner.pool_id = options.pool_id;

        STATS.lock().insert(
            id,
            ConnectedServer {
                stats,
                addr: addr.clone(),
                application_name: params.get_default("application_name", "PgDog").to_owned(),
                client: None,
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

    pub fn link_client(&mut self, client_name: &str, server_server: &str, id: &BackendKeyData) {
        self.client_id = Some(*id);
        if client_name != server_server {
            let mut guard = STATS.lock();
            if let Some(entry) = guard.get_mut(&self.id) {
                entry.application_name.clear();
                entry.application_name.push_str(client_name);
            }
        }
    }

    pub fn parse_complete(&mut self) {
        self.total.parse += 1;
        self.last_checkout.parse += 1;
        self.total.prepared_statements += 1;
        self.last_checkout.prepared_statements += 1;
    }

    /// Overwrite how many prepared statements we have in the cache
    /// for stats.
    pub fn set_prepared_statements(&mut self, size: usize) {
        self.total.prepared_statements = size;
        self.total.prepared_sync += 1;
        self.last_checkout.prepared_sync += 1;
        self.update();
    }

    pub fn close_many(&mut self, closed: usize, size: usize) {
        self.total.prepared_statements = size;
        self.total.close += closed;
        self.last_checkout.close += closed;
        self.update();
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

    /// Increment two-phase commit transaction count.
    pub fn transaction_2pc(&mut self) {
        self.last_checkout.transactions_2pc += 1;
        self.total.transactions_2pc += 1;
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
    pub fn query(&mut self, now: Instant, idle_in_transaction: bool) {
        self.total.queries += 1;
        self.last_checkout.queries += 1;

        if idle_in_transaction {
            self.idle_in_transaction_timer = Some(now);
        }

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
        // Client started a query/transaction.
        if self.state == State::Active {
            let now = Instant::now();
            if self.transaction_timer.is_none() {
                self.transaction_timer = Some(now);
            }
            if self.query_timer.is_none() {
                self.query_timer = Some(now);
            }
            if let Some(idle_in_transaction_timer) = self.idle_in_transaction_timer.take() {
                let elapsed = now.duration_since(idle_in_transaction_timer);
                self.last_checkout.idle_in_transaction_time += elapsed;
                self.total.idle_in_transaction_time += elapsed;
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

    /// Track healtchecks.
    pub fn healthcheck(&mut self) {
        self.total.healthchecks += 1;
        self.last_checkout.healthchecks += 1;
        self.last_healthcheck = Some(Instant::now());
        self.update();
    }

    #[inline]
    pub fn memory_used(&mut self, stats: MemoryStats) {
        self.memory = *stats;
    }

    #[inline]
    pub fn cleaned(&mut self) {
        self.last_checkout.cleaned += 1;
        self.total.cleaned += 1;
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
