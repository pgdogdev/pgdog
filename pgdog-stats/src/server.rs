use crate::memory::MemoryStats;
use crate::pool::Counts as PoolCounts;
use crate::state::State;
use serde::{Deserialize, Serialize};
use std::{
    ops::Add,
    time::{Duration, SystemTime},
};

/// Server connection stats.
#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize)]
pub struct Counts {
    pub bytes_sent: usize,
    pub bytes_received: usize,
    pub transactions: usize,
    pub transactions_2pc: usize,
    pub queries: usize,
    pub rollbacks: usize,
    pub errors: usize,
    pub prepared_statements: usize,
    pub query_time: Duration,
    pub transaction_time: Duration,
    pub idle_in_transaction_time: Duration,
    pub parse: usize,
    pub bind: usize,
    pub healthchecks: usize,
    pub close: usize,
    pub cleaned: usize,
    pub prepared_sync: usize,
}

impl Add<Counts> for PoolCounts {
    type Output = PoolCounts;

    fn add(self, rhs: Counts) -> Self::Output {
        PoolCounts {
            xact_count: self.xact_count + rhs.transactions,
            xact_2pc_count: self.xact_2pc_count + rhs.transactions_2pc,
            query_count: self.query_count + rhs.queries,
            server_assignment_count: self.server_assignment_count,
            received: self.received + rhs.bytes_received,
            sent: self.sent + rhs.bytes_sent,
            query_time: self.query_time + rhs.query_time,
            xact_time: self.xact_time + rhs.transaction_time,
            idle_xact_time: self.idle_xact_time + rhs.idle_in_transaction_time,
            wait_time: self.wait_time,
            parse_count: self.parse_count + rhs.parse,
            bind_count: self.bind_count + rhs.bind,
            rollbacks: self.rollbacks + rhs.rollbacks,
            healthchecks: self.healthchecks + rhs.healthchecks,
            close: self.close + rhs.close,
            errors: self.errors + rhs.errors,
            cleaned: self.cleaned + rhs.cleaned,
            prepared_sync: self.prepared_sync + rhs.prepared_sync,
            connect_count: self.connect_count,
            connect_time: self.connect_time,
        }
    }
}

impl Add for Counts {
    type Output = Counts;

    fn add(self, rhs: Self) -> Self::Output {
        Counts {
            bytes_sent: self.bytes_sent.saturating_add(rhs.bytes_sent),
            bytes_received: self.bytes_received.saturating_add(rhs.bytes_received),
            transactions: self.transactions.saturating_add(rhs.transactions),
            transactions_2pc: self.transactions_2pc.saturating_add(rhs.transactions_2pc),
            queries: self.queries.saturating_add(rhs.queries),
            rollbacks: self.rollbacks.saturating_add(rhs.rollbacks),
            errors: self.errors.saturating_add(rhs.errors),
            prepared_statements: self.prepared_statements + rhs.prepared_statements,
            query_time: self.query_time.saturating_add(rhs.query_time),
            transaction_time: self.transaction_time.saturating_add(rhs.transaction_time),
            idle_in_transaction_time: self
                .idle_in_transaction_time
                .saturating_add(rhs.idle_in_transaction_time),
            parse: self.parse.saturating_add(rhs.parse),
            bind: self.bind.saturating_add(rhs.bind),
            healthchecks: self.healthchecks.saturating_add(rhs.healthchecks),
            close: self.close.saturating_add(rhs.close),
            cleaned: self.cleaned.saturating_add(rhs.cleaned),
            prepared_sync: self.prepared_sync.saturating_add(rhs.prepared_sync),
        }
    }
}

/// Server statistics.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct Stats {
    pub state: State,
    pub created_at_time: SystemTime,
    pub total: Counts,
    pub last_checkout: Counts,
    pub pool_id: u64,
    pub memory: MemoryStats,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            state: State::Idle,
            created_at_time: SystemTime::now(),
            total: Counts::default(),
            last_checkout: Counts::default(),
            pool_id: 0,
            memory: MemoryStats::default(),
        }
    }
}
