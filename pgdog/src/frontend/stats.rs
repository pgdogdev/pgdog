//! Frontend client statistics.

use std::time::{Duration, Instant};

use crate::state::State;

/// Client statistics.
#[derive(Copy, Clone, Debug)]
pub struct Stats {
    /// Bytes sent over network.
    pub bytes_sent: usize,
    /// Bytes received over network.
    pub bytes_received: usize,
    /// Transactions served.
    pub transactions: usize,
    /// Queries served.
    pub queries: usize,
    /// Errors.
    pub errors: usize,
    /// Total transaction time.
    pub transaction_time: Duration,
    /// Total query time.
    pub query_time: Duration,
    /// Total wait time.
    pub wait_time: Duration,
    /// Current client state.
    pub state: State,
    transaction_timer: Instant,
    query_timer: Instant,
    wait_timer: Instant,
}

impl Stats {
    pub(super) fn new() -> Self {
        let now = Instant::now();
        Self {
            bytes_sent: 0,
            bytes_received: 0,
            transactions: 0,
            queries: 0,
            errors: 0,
            transaction_time: Duration::from_secs(0),
            query_time: Duration::from_secs(0),
            wait_time: Duration::from_secs(0),
            state: State::Idle,
            transaction_timer: now,
            query_timer: now,
            wait_timer: now,
        }
    }

    pub(super) fn transaction(&mut self) -> Self {
        self.transactions += 1;
        self.transaction_time += self.transaction_timer.elapsed();
        self.state = State::Idle;
        *self
    }

    pub(super) fn error(&mut self) -> Self {
        self.errors += 1;
        self.state = State::Idle;
        *self
    }

    pub(super) fn query(&mut self) -> Self {
        let now = Instant::now();
        self.queries += 1;
        self.query_time += now.duration_since(self.query_timer);
        self.query_timer = now;
        *self
    }

    pub(super) fn waiting(&mut self) -> Self {
        self.state = State::Waiting;
        self.wait_timer = Instant::now();
        *self
    }

    /// Get wait time if waiting.
    pub fn wait_time(&self) -> Duration {
        if self.state == State::Waiting {
            self.wait_timer.elapsed()
        } else {
            Duration::from_secs(0)
        }
    }

    pub(super) fn connected(&mut self) -> Self {
        let now = Instant::now();
        self.state = State::Active;
        self.transaction_timer = now;
        self.query_timer = now;
        self.wait_time = now.duration_since(self.wait_timer);
        *self
    }

    pub(super) fn sent(&mut self, bytes: usize) -> Self {
        self.bytes_sent += bytes;
        *self
    }

    pub(super) fn received(&mut self, bytes: usize) -> Self {
        self.bytes_received += bytes;
        *self
    }
}
