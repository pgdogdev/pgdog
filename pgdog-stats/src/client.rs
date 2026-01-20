use serde::{Deserialize, Serialize};

use crate::{MemoryStats, state::State};

use std::ops::Add;
use std::time::Duration;
use std::time::SystemTime;

/// Client statistics.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
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

impl Stats {
    pub fn new() -> Self {
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
            last_request: SystemTime::now(),
            memory_stats: MemoryStats::default(),
            prepared_statements: 0,
            locked: false,
        }
    }
}

impl Add for Stats {
    type Output = Stats;

    fn add(self, rhs: Self) -> Self::Output {
        Stats {
            bytes_sent: self.bytes_sent.saturating_add(rhs.bytes_sent),
            bytes_received: self.bytes_received.saturating_add(rhs.bytes_received),
            transactions: self.transactions.saturating_add(rhs.transactions),
            transactions_2pc: self.transactions_2pc.saturating_add(rhs.transactions_2pc),
            queries: self.queries.saturating_add(rhs.queries),
            errors: self.errors.saturating_add(rhs.errors),
            transaction_time: self.transaction_time.saturating_add(rhs.transaction_time),
            last_transaction_time: self.last_transaction_time.max(rhs.last_transaction_time),
            query_time: self.query_time.saturating_add(rhs.query_time),
            wait_time: self.wait_time.saturating_add(rhs.wait_time),
            state: rhs.state, // Not summed
            last_request: self.last_request.max(rhs.last_request),
            memory_stats: self.memory_stats + rhs.memory_stats,
            prepared_statements: self.prepared_statements + rhs.prepared_statements,
            locked: rhs.locked, // Not summed either
        }
    }
}
