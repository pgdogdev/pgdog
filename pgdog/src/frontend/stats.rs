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
pub(crate) struct Stats {
    inner: StatsStats,
    transaction_timer: Instant,
    query_timer: Instant,
    wait_timer: Instant,
    last_request_at: Instant,
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
            last_request_at: now,
        }
    }

    pub(super) fn transaction(&mut self, two_pc: bool, now: Instant) {
        self.last_transaction_time = now.duration_since(self.transaction_timer);
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

    pub(super) fn query(&mut self, now: Instant) {
        self.queries += 1;
        self.inner.query_time += now.duration_since(self.query_timer);
        self.query_timer = now;
    }

    pub(super) fn waiting(&mut self, instant: Instant) {
        self.state = State::Waiting;
        self.wait_timer = instant;
    }

    /// Get wait time if waiting.
    pub(crate) fn wait_time(&self) -> Duration {
        if self.state == State::Waiting {
            self.wait_timer.elapsed()
        } else {
            Duration::from_secs(0)
        }
    }

    pub(super) fn connected(&mut self, connected_at: Instant) {
        self.state = State::Active;
        self.transaction_timer = connected_at;
        self.query_timer = connected_at;
        self.wait_time = connected_at.duration_since(self.wait_timer);
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

    pub(super) fn received(&mut self, bytes: usize, request_start: Instant) {
        self.bytes_received += bytes;
        // In session mode, we stay connected to the server
        // until client disconnects, so we need to reset timers every time
        // client is activated from idle state.
        if self.state == State::Idle {
            self.transaction_timer = request_start;
            self.query_timer = request_start;
            self.last_request_at = request_start;
        }

        self.state = State::Active;
    }

    pub(crate) fn last_request(&self) -> SystemTime {
        SystemTime::now()
            .checked_sub(self.last_request_at.elapsed())
            .unwrap_or(SystemTime::UNIX_EPOCH)
    }

    /// Number of prepared statements currently in the cache.
    pub(super) fn prepared_statements(&mut self, prepared: usize) {
        self.prepared_statements = prepared;
    }
}
