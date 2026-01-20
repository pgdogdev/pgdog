//! Frontend client statistics.

use std::{
    ops::{Deref, DerefMut},
    time::{Duration, SystemTime},
};
use tokio::time::Instant;

use crate::{backend::pool::stats::MemoryStats, state::State};
use pgdog_stats::client::Stats as StatsStats;

/// Client statistics.
#[derive(Copy, Clone, Debug)]
pub struct Stats {
    inner: StatsStats,
    transaction_timer: Instant,
    query_timer: Instant,
    wait_timer: Instant,
}

impl Deref for Stats {
    type Target = StatsStats;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Stats {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
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
            inner: StatsStats::new(),
            transaction_timer: now,
            query_timer: now,
            wait_timer: now,
        }
    }

    pub(super) fn transaction(&mut self, two_pc: bool) {
        self.last_transaction_time = self.transaction_timer.elapsed();
        self.transactions += 1;
        self.inner.transaction_time += self.inner.last_transaction_time;
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
        self.inner.query_time += now.duration_since(self.query_timer);
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
        self.memory_stats = *memory;
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
