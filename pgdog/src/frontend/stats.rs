//! Frontend client statistics.

use std::time::{Duration, SystemTime};
use tokio::time::Instant;

use crate::{backend::pool::stats::MemoryStats, state::State};

/// Client statistics.
#[derive(Copy, Clone, Debug)]
pub struct Stats {
    /// Bytes sent over network.
    pub bytes_sent: usize,
    /// Bytes received over network.
    pub bytes_received: usize,
    /// Transactions served.
    pub transactions: usize,
    /// Two-pc transactions.
    pub transactions_2pc: usize,
    /// Queries served.
    pub queries: usize,
    /// Errors.
    pub errors: usize,
    /// Total transaction time.
    pub transaction_time: Duration,
    /// Last transaction time.
    pub last_transaction_time: Duration,
    /// Total query time.
    pub query_time: Duration,
    /// Total wait time.
    pub wait_time: Duration,
    /// Current client state.
    pub state: State,
    transaction_timer: Instant,
    query_timer: Instant,
    wait_timer: Instant,
    /// Last time this client sent a query.
    pub last_request: SystemTime,
    /// Number of bytes used by the stream buffer, where all the messages
    /// are stored until they are processed.
    pub memory_stats: MemoryStats,
    /// Number of prepared statements in the local cache.
    pub prepared_statements: usize,
    /// Client is locked to a particular server.
    pub locked: bool,
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

impl Stats {
    pub(super) fn new() -> Self {
        let now = Instant::now();
        Self {
            bytes_sent: 0,
            bytes_received: 0,
            transactions: 0,
            transactions_2pc: 0,
            queries: 0,
            errors: 0,
            transaction_time: Duration::from_secs(0),
            last_transaction_time: Duration::from_secs(0),
            query_time: Duration::from_secs(0),
            wait_time: Duration::from_secs(0),
            state: State::Idle,
            transaction_timer: now,
            query_timer: now,
            wait_timer: now,
            last_request: SystemTime::now(),
            memory_stats: MemoryStats::default(),
            prepared_statements: 0,
            locked: false,
        }
    }

    pub(super) fn transaction(&mut self, two_pc: bool) {
        self.last_transaction_time = self.transaction_timer.elapsed();
        self.transactions += 1;
        self.transaction_time += self.last_transaction_time;
        if two_pc {
            self.transactions_2pc += 1;
        }
        self.state = State::Idle;
    }

    pub(super) fn error(&mut self) {
        self.errors += 1;
        self.state = State::Idle;
    }

    pub(super) fn query(&mut self) {
        let now = Instant::now();
        self.queries += 1;
        self.query_time += now.duration_since(self.query_timer);
        self.query_timer = now;
    }

    pub(super) fn waiting(&mut self, instant: Instant) {
        self.state = State::Waiting;
        self.wait_timer = instant;
    }

    /// Get wait time if waiting.
    pub fn wait_time(&self) -> Duration {
        if self.state == State::Waiting {
            self.wait_timer.elapsed()
        } else {
            Duration::from_secs(0)
        }
    }

    pub(super) fn connected(&mut self) {
        let now = Instant::now();
        self.state = State::Active;
        self.transaction_timer = now;
        self.query_timer = now;
        self.wait_time = now.duration_since(self.wait_timer);
    }

    pub(super) fn locked(&mut self, lock: bool) {
        self.locked = lock;
    }

    pub(super) fn sent(&mut self, bytes: usize) {
        self.bytes_sent += bytes;
    }

    pub(super) fn memory_used(&mut self, memory: MemoryStats) {
        self.memory_stats = memory;
    }

    pub(super) fn idle(&mut self, in_transaction: bool) {
        if in_transaction {
            self.state = State::IdleInTransaction;
        } else {
            self.state = State::Idle;
        }
    }

    pub(super) fn received(&mut self, bytes: usize) {
        self.bytes_received += bytes;
        // In session mode, we stay connected to the server
        // until client disconnects, so we need to reset timers every time
        // client is activated from idle state.
        if self.state == State::Idle {
            let now = Instant::now();
            self.transaction_timer = now;
            self.query_timer = now;
            self.last_request = SystemTime::now();
        }

        self.state = State::Active;
    }

    /// Number of prepared statements currently in the cache.
    pub(super) fn prepared_statements(&mut self, prepared: usize) {
        self.prepared_statements = prepared;
    }
}
