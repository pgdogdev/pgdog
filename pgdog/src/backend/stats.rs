//! Keep track of server stats.

use std::sync::Arc;
use std::time::SystemTime;

use fnv::FnvHashMap as HashMap;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use pgdog_stats::memory::MemoryStats as StatsMemoryStats;
pub(crate) use pgdog_stats::server::Counts;
use tokio::time::Instant;

use crate::{
    backend::{Pool, pool::stats::MemoryStats},
    config::Memory,
    frontend::router::parser::{StatementType, TableChange},
    net::{
        Parameters,
        messages::{BackendPid, CommandComplete, FrontendPid},
    },
    state::State,
};

use super::pool::Address;

static STATS: Lazy<RwLock<HashMap<BackendPid, Arc<Mutex<ConnectedServer>>>>> =
    Lazy::new(|| RwLock::new(HashMap::default()));

/// Get a snapshot of all connected-server stats.
pub(crate) fn stats() -> Vec<ConnectedServer> {
    STATS.read().values().map(|v| v.lock().clone()).collect()
}

/// Get idle-in-transaction server connections for connection pool.
pub(crate) fn idle_in_transaction(pool: &Pool) -> usize {
    STATS
        .read()
        .values()
        .filter(|stat| {
            let guard = stat.lock();
            guard.stats.pool_id == pool.id() && guard.stats.state == State::IdleInTransaction
        })
        .count()
}

#[derive(Clone, Debug, Copy, Default)]
pub(crate) struct SharedCounts {
    pub(crate) bytes_sent: usize,
    pub(crate) bytes_received: usize,
    pub(crate) transactions: usize,
    pub(crate) queries: usize,
    pub(crate) rollbacks: usize,
    pub(crate) errors: usize,
    pub(crate) healthchecks: usize,
    pub(crate) prepared_statements: usize,
}

// All server connections keep a stats handle (Stats) with: (1) a local copy of the stats (ServerStats), which the connection updates, and (2) a shared copy behind a mutex (SharedServerStats).
// Admin commands, e.g., SHOW SERVERS and SHOW POOLS, use (2).
// SharedServerStats is a minimal set of fields representing only what (2) needs to ensure that we only copy those fields under the mutex (and not all fields of (1)).
#[derive(Clone, Debug, Copy)]
pub(crate) struct SharedServerStats {
    pub(crate) id: BackendPid,
    pub(crate) client_id: Option<FrontendPid>,
    pub(crate) state: State,
    pub(crate) pool_id: u64,
    pub(crate) created_at: Instant,
    pub(crate) created_at_time: SystemTime,
    pub(crate) last_used: Instant,
    pub(crate) last_healthcheck: Option<Instant>,
    pub(crate) last_sent: u8,
    pub(crate) last_received: u8,
    pub(crate) memory: StatsMemoryStats,
    pub(crate) total: SharedCounts,
}

mod local {
    use tokio::time::Instant;

    use super::{Counts, SharedServerStats};

    // The shared field of ServerStats is private to this module, so all changes to the values in that field go through shared_mut(), which sets the dirty flag (except in sent() and received(), since sent() and received() also copy the byte count and the last message code to the shared copy right away).
    // This is necessary as sent() and received() only copy the byte count and the last message code when dirty isn't set, so if a change skipped dirty, the shared copy would keep the old value (e.g., the old queries count after query()).
    #[derive(Clone, Debug, Copy)]
    pub(super) struct ServerStats {
        shared: SharedServerStats,
        dirty: bool,
        pub(super) last_checkout: Counts,
        pub(super) query_timer: Option<Instant>,
        pub(super) transaction_timer: Option<Instant>,
        pub(super) idle_in_transaction_timer: Option<Instant>,
    }

    impl ServerStats {
        pub(super) fn new(shared: SharedServerStats) -> Self {
            Self {
                shared,
                dirty: false,
                last_checkout: Counts::default(),
                query_timer: None,
                transaction_timer: None,
                idle_in_transaction_timer: None,
            }
        }

        pub(super) fn shared(&self) -> &SharedServerStats {
            &self.shared
        }

        pub(super) fn shared_mut(&mut self) -> &mut SharedServerStats {
            self.dirty = true;
            &mut self.shared
        }

        pub(super) fn sync_to(&mut self, destination: &mut SharedServerStats) {
            *destination = self.shared;
            self.dirty = false;
        }

        pub(super) fn sent(&mut self, bytes: usize, code: u8, destination: &mut SharedServerStats) {
            self.shared.total.bytes_sent += bytes;
            self.shared.last_sent = code;

            if self.dirty {
                self.sync_to(destination);
            } else {
                destination.total.bytes_sent = self.shared.total.bytes_sent;
                destination.last_sent = code;
            }
        }

        pub(super) fn received(
            &mut self,
            bytes: usize,
            code: u8,
            destination: &mut SharedServerStats,
        ) {
            self.shared.total.bytes_received += bytes;
            self.shared.last_received = code;

            if self.dirty {
                self.sync_to(destination);
            } else {
                destination.total.bytes_received = self.shared.total.bytes_received;
                destination.last_received = code;
            }
        }
    }
}

use local::ServerStats;

/// Connected server (shared globally).
#[derive(Clone, Debug)]
pub(crate) struct ConnectedServer {
    pub(crate) stats: SharedServerStats,
    pub(crate) addr: Address,
    pub(crate) application_name: String,
}

/// Server statistics handle.
///
/// Holds local stats for fast reads during pool operations,
/// and a reference to shared stats for global visibility.
/// Syncs local to shared on I/O operations.
#[derive(Clone, Debug)]
pub(crate) struct Stats {
    local: ServerStats,
    shared: Arc<Mutex<ConnectedServer>>,
}

impl Stats {
    /// Register new server with statistics.
    pub(crate) fn connect(
        id: BackendPid,
        addr: &Address,
        params: &Parameters,
        config: &Memory,
    ) -> Self {
        let now = Instant::now();
        let local = ServerStats::new(SharedServerStats {
            id,
            client_id: None,
            state: State::Idle,
            pool_id: 0,
            created_at: now,
            created_at_time: SystemTime::now(),
            last_used: now,
            last_healthcheck: None,
            last_sent: 0,
            last_received: 0,
            memory: *MemoryStats::new(config),
            total: SharedCounts::default(),
        });

        let server = ConnectedServer {
            stats: *local.shared(),
            addr: addr.clone(),
            application_name: params.get_default("application_name", "PgDog").to_owned(),
        };

        let shared = Arc::new(Mutex::new(server));
        STATS.write().insert(id, Arc::clone(&shared));

        Stats { local, shared }
    }

    /// Copies all fields of SharedServerStats from the local copy to the shared copy and clears dirty.
    fn sync_to_shared(&mut self) {
        self.local.sync_to(&mut self.shared.lock().stats);
    }

    fn transaction_state(&mut self, now: Instant, state: State) {
        let shared = self.local.shared_mut();
        shared.total.transactions += 1;
        shared.state = state;
        shared.last_used = now;

        self.local.last_checkout.transactions += 1;
        if let Some(transaction_timer) = self.local.transaction_timer.take() {
            let duration = now.duration_since(transaction_timer);
            self.local.last_checkout.transaction_time += duration;
        }
        self.sync_to_shared();
    }

    pub(crate) fn link_client(&mut self, client_name: &str, server_name: &str, id: FrontendPid) {
        self.local.shared_mut().client_id = Some(id);
        if client_name != server_name {
            let mut guard = self.shared.lock();
            guard.stats.client_id = Some(id);
            guard.application_name.clear();
            guard.application_name.push_str(client_name);
        }
    }

    pub(crate) fn parse_complete(&mut self) {
        self.local.shared_mut().total.prepared_statements += 1;
        self.local.last_checkout.parse += 1;
        self.local.last_checkout.prepared_statements += 1;
    }

    /// Overwrite how many prepared statements we have in the cache for stats.
    pub(crate) fn set_prepared_statements(&mut self, size: usize) {
        self.local.shared_mut().total.prepared_statements = size;
        self.local.last_checkout.prepared_sync += 1;
        self.sync_to_shared();
    }

    pub(crate) fn close_many(&mut self, closed: usize, size: usize) {
        self.local.shared_mut().total.prepared_statements = size;
        self.local.last_checkout.close += closed;
        self.sync_to_shared();
    }

    pub(crate) fn copy_mode(&mut self) {
        self.state(State::CopyMode);
    }

    pub(crate) fn bind_complete(&mut self) {
        self.local.last_checkout.bind += 1;
    }

    /// Record rows affected from a Postgres CommandComplete message.
    pub(crate) fn rows_affected(&mut self, cmd: &CommandComplete) {
        let Some(rows) = cmd.rows() else {
            return;
        };
        match cmd.tag() {
            "INSERT" => {
                self.local.last_checkout.rows_inserted += rows;
            }
            "UPDATE" => {
                self.local.last_checkout.rows_updated += rows;
            }
            "DELETE" => {
                self.local.last_checkout.rows_deleted += rows;
            }
            "COPY" => {
                self.local.last_checkout.rows_copied += rows;
            }
            _ => {}
        }
    }

    /// Record a successfully executed statement classified by the query parser.
    pub(crate) fn statement_completed(
        &mut self,
        statement_type: StatementType,
        table_change: TableChange,
    ) {
        match statement_type {
            StatementType::Ddl => {
                self.local.last_checkout.ddl_commands += 1;
            }
            StatementType::Dml => {
                self.local.last_checkout.dml_commands += 1;
            }
            StatementType::SessionControl | StatementType::TransactionControl => {}
        }

        match table_change {
            TableChange::Created => {
                self.local.last_checkout.tables_created += 1;
            }
            TableChange::Dropped(tables) => {
                self.local.last_checkout.tables_dropped += tables as usize;
            }
            TableChange::None => {}
        }
    }

    /// A transaction has been completed.
    pub(crate) fn transaction(&mut self, now: Instant) {
        self.transaction_state(now, State::Idle);
    }

    /// Increment two-phase commit transaction count.
    pub(crate) fn transaction_2pc(&mut self) {
        self.local.last_checkout.transactions_2pc += 1;
    }

    /// Error occurred in a transaction.
    pub(crate) fn transaction_error(&mut self, now: Instant) {
        self.transaction_state(now, State::TransactionError);
    }

    /// An error occurred in general.
    pub(crate) fn error(&mut self) {
        self.local.shared_mut().total.errors += 1;
        self.local.last_checkout.errors += 1;
    }

    /// Postgres disconnected this connection because it exceeded
    /// `idle_in_transaction_session_timeout`.
    pub(crate) fn idle_xact_timeout(&mut self) {
        self.local.last_checkout.idle_xact_timeouts += 1;
    }

    /// A query has been completed.
    pub(crate) fn query(&mut self, now: Instant, idle_in_transaction: bool) {
        self.local.shared_mut().total.queries += 1;
        self.local.last_checkout.queries += 1;

        if idle_in_transaction {
            self.local.idle_in_transaction_timer = Some(now);
        }

        if let Some(query_timer) = self.local.query_timer.take() {
            let duration = now.duration_since(query_timer);
            self.local.last_checkout.query_time += duration;
        }
    }

    pub(crate) fn set_timers(&mut self, now: Instant) {
        self.local.transaction_timer = Some(now);
        self.local.query_timer = Some(now);
    }

    /// Manual state change.
    pub(crate) fn state(&mut self, state: State) {
        if self.local.shared().state != state {
            self.local.shared_mut().state = state;
            if state == State::Active
                && (self.local.transaction_timer.is_none()
                    || self.local.query_timer.is_none()
                    || self.local.idle_in_transaction_timer.is_some())
            {
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
                }
            }
            self.sync_to_shared();
        }
    }

    /// Send bytes to server.
    /// If dirty is set, we copy all fields of SharedServerStats to the shared copy; otherwise, we only copy the byte count and the last message code.
    pub(crate) fn send(&mut self, bytes: usize, code: u8) {
        self.local.last_checkout.bytes_sent += bytes;
        self.local.sent(bytes, code, &mut self.shared.lock().stats);
    }

    /// Receive bytes from server.
    /// If dirty is set, we copy all fields of SharedServerStats to the shared copy; otherwise, we only copy the byte count and the last message code.
    pub(crate) fn receive(&mut self, bytes: usize, code: u8) {
        self.local.last_checkout.bytes_received += bytes;
        self.local
            .received(bytes, code, &mut self.shared.lock().stats);
    }

    /// Track healthchecks.
    pub(crate) fn healthcheck(&mut self) {
        let shared = self.local.shared_mut();
        shared.total.healthchecks += 1;
        shared.last_healthcheck = Some(Instant::now());

        self.local.last_checkout.healthchecks += 1;
        self.sync_to_shared();
    }

    pub(crate) fn memory_used(&mut self, stats: MemoryStats) {
        self.local.shared_mut().memory = *stats;
    }

    pub(crate) fn cleaned(&mut self) {
        self.local.last_checkout.cleaned += 1;
    }

    /// Track rollbacks.
    pub(crate) fn rollback(&mut self) {
        self.local.shared_mut().total.rollbacks += 1;
        self.local.last_checkout.rollbacks += 1;
        self.sync_to_shared();
    }

    /// Server is closing.
    pub(super) fn disconnect(&self) {
        STATS.write().remove(&self.local.shared().id);
    }

    /// Reset last_checkout counts.
    pub(crate) fn reset_last_checkout(&mut self) -> Counts {
        let counts = self.local.last_checkout;
        self.local.last_checkout = Counts::default();
        counts
    }

    // Fast accessor methods - read from local, no locking.

    /// Get current state (local, no lock).
    pub(crate) fn get_state(&self) -> State {
        self.local.shared().state
    }

    /// Get created_at timestamp (local, no lock).
    pub(crate) fn created_at(&self) -> Instant {
        self.local.shared().created_at
    }

    /// Get last_used timestamp (local, no lock).
    pub(crate) fn last_used(&self) -> Instant {
        self.local.shared().last_used
    }

    /// Get last_healthcheck timestamp (local, no lock).
    pub(crate) fn last_healthcheck(&self) -> Option<Instant> {
        self.local.shared().last_healthcheck
    }

    /// Get pool_id (local, no lock).
    #[cfg(test)]
    pub(crate) fn pool_id(&self) -> u64 {
        self.local.shared().pool_id
    }

    /// Set pool_id.
    pub(crate) fn set_pool_id(&mut self, pool_id: u64) {
        self.local.shared_mut().pool_id = pool_id;
        self.sync_to_shared();
    }

    /// Get total counts (local, no lock).
    #[cfg(test)]
    pub(crate) fn total(&self) -> SharedCounts {
        self.local.shared().total
    }

    /// Get last_checkout counts (local, no lock).
    #[cfg(test)]
    pub(crate) fn last_checkout(&self) -> Counts {
        self.local.last_checkout
    }

    /// Clear client_id.
    pub(crate) fn clear_client_id(&mut self) {
        self.local.shared_mut().client_id = None;
    }

    /// Legacy update method - syncs local to shared.
    pub(crate) fn update(&mut self) {
        self.sync_to_shared();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontend::router::parser::{StatementType, TableChange};

    fn stats() -> Stats {
        Stats::connect(
            BackendPid::for_test(1),
            &Address::default(),
            &Parameters::default(),
            &Memory::default(),
        )
    }

    #[test]
    fn copy_rows_are_counted() {
        let mut stats = stats();
        stats.rows_affected(&CommandComplete::from_str("COPY 42"));

        assert_eq!(stats.last_checkout().rows_copied, 42);
    }

    #[test]
    fn parsed_statement_metrics_are_counted() {
        let mut stats = stats();
        stats.statement_completed(StatementType::Dml, TableChange::None);
        stats.statement_completed(StatementType::Ddl, TableChange::Created);
        stats.statement_completed(StatementType::Ddl, TableChange::Dropped(2));

        assert_eq!(stats.last_checkout().dml_commands, 1);
        assert_eq!(stats.last_checkout().ddl_commands, 2);
        assert_eq!(stats.last_checkout().tables_created, 1);
        assert_eq!(stats.last_checkout().tables_dropped, 2);
    }

    #[test]
    fn shared_copy_shows_query_after_the_next_send() {
        let mut stats = stats();
        stats.query(Instant::now(), false);
        assert_eq!(stats.shared.lock().stats.total.queries, 0);

        stats.send(10, b'Q');

        assert_eq!(stats.shared.lock().stats.total.queries, 1);
    }
}
