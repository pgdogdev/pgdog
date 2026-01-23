//! Keep track of server stats.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use fnv::FnvHashMap as HashMap;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
pub use pgdog_stats::server::Counts;
use tokio::time::Instant;

use crate::{
    backend::{pool::stats::MemoryStats, Pool, ServerOptions},
    config::Memory,
    net::{messages::BackendKeyData, Parameters},
    state::State,
};

use super::pool::Address;

static STATS: Lazy<RwLock<HashMap<BackendKeyData, Arc<Mutex<ConnectedServer>>>>> =
    Lazy::new(|| RwLock::new(HashMap::default()));

/// Get a copy of latest stats.
pub fn stats() -> HashMap<BackendKeyData, ConnectedServer> {
    STATS
        .read()
        .iter()
        .map(|(k, v)| (*k, v.lock().clone()))
        .collect()
}

/// Get idle-in-transaction server connections for connection pool.
pub fn idle_in_transaction(pool: &Pool) -> usize {
    STATS
        .read()
        .values()
        .filter(|stat| {
            let guard = stat.lock();
            guard.stats.pool_id == pool.id() && guard.stats.state == State::IdleInTransaction
        })
        .count()
}

/// Core server statistics (shared between local and global).
#[derive(Clone, Debug, Copy)]
pub struct ServerStats {
    pub inner: pgdog_stats::server::Stats,
    pub id: BackendKeyData,
    pub last_used: Instant,
    pub last_healthcheck: Option<Instant>,
    pub created_at: Instant,
    pub client_id: Option<BackendKeyData>,
    query_timer: Option<Instant>,
    transaction_timer: Option<Instant>,
    idle_in_transaction_timer: Option<Instant>,
}

impl ServerStats {
    fn new(id: BackendKeyData, options: &ServerOptions, config: &Memory) -> Self {
        let now = Instant::now();
        let mut inner = pgdog_stats::server::Stats::default();
        inner.memory = *MemoryStats::new(config);
        inner.pool_id = options.pool_id;

        Self {
            inner,
            id,
            last_used: now,
            last_healthcheck: None,
            created_at: now,
            client_id: None,
            query_timer: None,
            transaction_timer: None,
            idle_in_transaction_timer: None,
        }
    }
}

impl Deref for ServerStats {
    type Target = pgdog_stats::server::Stats;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for ServerStats {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// Connected server (shared globally).
#[derive(Clone, Debug)]
pub struct ConnectedServer {
    pub stats: ServerStats,
    pub addr: Address,
    pub application_name: String,
    pub client: Option<BackendKeyData>,
}

/// Server statistics handle.
///
/// Holds local stats for fast reads during pool operations,
/// and a reference to shared stats for global visibility.
/// Syncs local to shared on I/O operations.
#[derive(Clone, Debug)]
pub struct Stats {
    local: ServerStats,
    shared: Arc<Mutex<ConnectedServer>>,
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
        let local = ServerStats::new(id, options, config);

        let server = ConnectedServer {
            stats: local.clone(),
            addr: addr.clone(),
            application_name: params.get_default("application_name", "PgDog").to_owned(),
            client: None,
        };

        let shared = Arc::new(Mutex::new(server));
        STATS.write().insert(id, Arc::clone(&shared));

        Stats { local, shared }
    }

    /// Sync local stats to shared (called on I/O operations).
    #[inline]
    fn sync_to_shared(&self) {
        self.shared.lock().stats = self.local.clone();
    }

    fn transaction_state(&mut self, now: Instant, state: State) {
        self.local.total.transactions += 1;
        self.local.last_checkout.transactions += 1;
        self.local.state = state;
        self.local.last_used = now;
        if let Some(transaction_timer) = self.local.transaction_timer.take() {
            let duration = now.duration_since(transaction_timer);
            self.local.total.transaction_time += duration;
            self.local.last_checkout.transaction_time += duration;
        }
        self.sync_to_shared();
    }

    pub fn link_client(&mut self, client_name: &str, server_name: &str, id: &BackendKeyData) {
        self.local.client_id = Some(*id);
        if client_name != server_name {
            let mut guard = self.shared.lock();
            guard.stats.client_id = self.local.client_id;
            guard.application_name.clear();
            guard.application_name.push_str(client_name);
        }
    }

    pub fn parse_complete(&mut self) {
        self.local.total.parse += 1;
        self.local.last_checkout.parse += 1;
        self.local.total.prepared_statements += 1;
        self.local.last_checkout.prepared_statements += 1;
    }

    /// Overwrite how many prepared statements we have in the cache for stats.
    pub fn set_prepared_statements(&mut self, size: usize) {
        self.local.total.prepared_statements = size;
        self.local.total.prepared_sync += 1;
        self.local.last_checkout.prepared_sync += 1;
        self.sync_to_shared();
    }

    pub fn close_many(&mut self, closed: usize, size: usize) {
        self.local.total.prepared_statements = size;
        self.local.total.close += closed;
        self.local.last_checkout.close += closed;
        self.sync_to_shared();
    }

    pub fn copy_mode(&mut self) {
        self.state(State::CopyMode);
    }

    pub fn bind_complete(&mut self) {
        self.local.total.bind += 1;
        self.local.last_checkout.bind += 1;
    }

    /// A transaction has been completed.
    pub fn transaction(&mut self, now: Instant) {
        self.transaction_state(now, State::Idle);
    }

    /// Increment two-phase commit transaction count.
    pub fn transaction_2pc(&mut self) {
        self.local.last_checkout.transactions_2pc += 1;
        self.local.total.transactions_2pc += 1;
    }

    /// Error occurred in a transaction.
    pub fn transaction_error(&mut self, now: Instant) {
        self.transaction_state(now, State::TransactionError);
    }

    /// An error occurred in general.
    pub fn error(&mut self) {
        self.local.total.errors += 1;
        self.local.last_checkout.errors += 1;
    }

    /// A query has been completed.
    pub fn query(&mut self, now: Instant, idle_in_transaction: bool) {
        self.local.total.queries += 1;
        self.local.last_checkout.queries += 1;

        if idle_in_transaction {
            self.local.idle_in_transaction_timer = Some(now);
        }

        if let Some(query_timer) = self.local.query_timer.take() {
            let duration = now.duration_since(query_timer);
            self.local.total.query_time += duration;
            self.local.last_checkout.query_time += duration;
        }
    }

    pub(crate) fn set_timers(&mut self, now: Instant) {
        self.local.transaction_timer = Some(now);
        self.local.query_timer = Some(now);
    }

    /// Manual state change.
    pub fn state(&mut self, state: State) {
        if self.local.state != state {
            self.local.state = state;
            if state == State::Active {
                let now = Instant::now();
                if self.local.transaction_timer.is_none() {
                    self.local.transaction_timer = Some(now);
                }
                if self.local.query_timer.is_none() {
                    self.local.query_timer = Some(now);
                }
                if let Some(idle_in_transaction_timer) = self.local.idle_in_transaction_timer.take()
                {
                    let elapsed = now.duration_since(idle_in_transaction_timer);
                    self.local.last_checkout.idle_in_transaction_time += elapsed;
                    self.local.total.idle_in_transaction_time += elapsed;
                }
            }
            self.sync_to_shared();
        }
    }

    /// Send bytes to server - syncs to shared for real-time visibility.
    pub fn send(&mut self, bytes: usize, code: u8) {
        self.local.total.bytes_sent += bytes;
        self.local.last_checkout.bytes_sent += bytes;
        self.local.last_sent = code;
        self.sync_to_shared();
    }

    /// Receive bytes from server - syncs to shared for real-time visibility.
    pub fn receive(&mut self, bytes: usize, code: u8) {
        self.local.total.bytes_received += bytes;
        self.local.last_checkout.bytes_received += bytes;
        self.local.last_received = code;
        self.sync_to_shared();
    }

    /// Track healthchecks.
    pub fn healthcheck(&mut self) {
        self.local.total.healthchecks += 1;
        self.local.last_checkout.healthchecks += 1;
        self.local.last_healthcheck = Some(Instant::now());
        self.sync_to_shared();
    }

    #[inline]
    pub fn memory_used(&mut self, stats: MemoryStats) {
        self.local.memory = *stats;
    }

    #[inline]
    pub fn cleaned(&mut self) {
        self.local.last_checkout.cleaned += 1;
        self.local.total.cleaned += 1;
    }

    /// Track rollbacks.
    pub fn rollback(&mut self) {
        self.local.total.rollbacks += 1;
        self.local.last_checkout.rollbacks += 1;
        self.sync_to_shared();
    }

    /// Server is closing.
    pub(super) fn disconnect(&self) {
        STATS.write().remove(&self.local.id);
    }

    /// Reset last_checkout counts.
    pub fn reset_last_checkout(&mut self) -> Counts {
        let counts = self.local.last_checkout;
        self.local.last_checkout = Counts::default();
        counts
    }

    // Fast accessor methods - read from local, no locking.

    /// Get current state (local, no lock).
    #[inline]
    pub fn get_state(&self) -> State {
        self.local.state
    }

    /// Get created_at timestamp (local, no lock).
    #[inline]
    pub fn created_at(&self) -> Instant {
        self.local.created_at
    }

    /// Get last_used timestamp (local, no lock).
    #[inline]
    pub fn last_used(&self) -> Instant {
        self.local.last_used
    }

    /// Get last_healthcheck timestamp (local, no lock).
    #[inline]
    pub fn last_healthcheck(&self) -> Option<Instant> {
        self.local.last_healthcheck
    }

    /// Get pool_id (local, no lock).
    #[inline]
    pub fn pool_id(&self) -> u64 {
        self.local.pool_id
    }

    /// Set pool_id.
    #[inline]
    pub fn set_pool_id(&mut self, pool_id: u64) {
        self.local.pool_id = pool_id;
        self.shared.lock().stats.pool_id = pool_id;
    }

    /// Get total counts (local, no lock).
    #[inline]
    pub fn total(&self) -> Counts {
        self.local.total
    }

    /// Get last_checkout counts (local, no lock).
    #[inline]
    pub fn last_checkout(&self) -> Counts {
        self.local.last_checkout
    }

    /// Clear client_id.
    #[inline]
    pub fn clear_client_id(&mut self) {
        self.local.client_id = None;
    }

    /// Legacy update method - syncs local to shared.
    #[inline]
    pub fn update(&self) {
        self.sync_to_shared();
    }
}
