//!
//! Pool statistics.
//!
//! Used in SHOW POOLS admin database command
//! and in Prometheus metrics.
//!

use crate::{backend::stats::Counts as BackendCounts, config::Memory, net::MessageBufferStats};

use std::{
    iter::Sum,
    ops::{Add, Div, Sub},
    time::Duration,
    u32,
};

/// Pool statistics.
///
/// These are updated after each connection check-in.
///
#[derive(Debug, Clone, Default, Copy)]
pub struct Counts {
    /// Number of committed transactions.
    pub xact_count: usize,
    /// Number of transactions committed with 2-phase commit.
    pub xact_2pc_count: usize,
    /// Number of executed queries.
    pub query_count: usize,
    /// How many times a server has been given to a client.
    /// In transaction mode, this equals to `xact_count`.
    pub server_assignment_count: usize,
    /// Number of bytes received by server connections.
    pub received: usize,
    /// Number of bytes sent to server connections.
    pub sent: usize,
    /// Total duration of all transactions.
    pub xact_time: Duration,
    /// Total time spent idling inside transactions.
    pub idle_xact_time: Duration,
    /// Total time spent executing queries.
    pub query_time: Duration,
    /// Total time clients spent waiting for a connection from the pool.
    pub wait_time: Duration,
    /// Total count of Parse messages sent to server connections.
    pub parse_count: usize,
    /// Total count of Bind messages sent to server connections.
    pub bind_count: usize,
    /// Number of times the pool had to rollback unfinished transactions.
    pub rollbacks: usize,
    /// Number of times the pool sent the health check query.
    pub healthchecks: usize,
    /// Total count of Close messages sent to server connections.
    pub close: usize,
    /// Total number of network-related errors detected on server connections.
    pub errors: usize,
    /// Total number of server connections that were cleaned after a dirty session.
    pub cleaned: usize,
    /// Total number of times servers had to synchronize prepared statements from Postgres'
    /// pg_prepared_statements view.
    pub prepared_sync: usize,
    /// Total time spent creating server connections.
    pub connect_time: Duration,
    /// Total number of times the pool attempted to create server connections.
    pub connect_count: usize,
}

impl Sub for Counts {
    type Output = Counts;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            xact_count: self.xact_count.saturating_sub(rhs.xact_count),
            xact_2pc_count: self.xact_2pc_count.saturating_sub(rhs.xact_2pc_count),
            query_count: self.query_count.saturating_sub(rhs.query_count),
            server_assignment_count: self
                .server_assignment_count
                .saturating_sub(rhs.server_assignment_count),
            received: self.received.saturating_sub(rhs.received),
            sent: self.sent.saturating_sub(rhs.sent),
            xact_time: self.xact_time.saturating_sub(rhs.xact_time),
            idle_xact_time: self.idle_xact_time.saturating_sub(rhs.idle_xact_time),
            query_time: self.query_time.saturating_sub(rhs.query_time),
            wait_time: self.wait_time.saturating_sub(rhs.wait_time),
            parse_count: self.parse_count.saturating_sub(rhs.parse_count),
            bind_count: self.bind_count.saturating_sub(rhs.bind_count),
            rollbacks: self.rollbacks.saturating_sub(rhs.rollbacks),
            healthchecks: self.healthchecks.saturating_sub(rhs.healthchecks),
            close: self.close.saturating_sub(rhs.close),
            errors: self.errors.saturating_sub(rhs.errors),
            cleaned: self.cleaned.saturating_sub(rhs.cleaned),
            prepared_sync: self.prepared_sync.saturating_sub(rhs.prepared_sync),
            connect_time: self.connect_time.saturating_sub(rhs.connect_time),
            connect_count: self.connect_count.saturating_sub(rhs.connect_count),
        }
    }
}

impl Div<usize> for Counts {
    type Output = Counts;

    fn div(self, rhs: usize) -> Self::Output {
        Self {
            xact_count: self.xact_count.checked_div(rhs).unwrap_or(0),
            xact_2pc_count: self.xact_2pc_count.checked_div(rhs).unwrap_or(0),
            query_count: self.query_count.checked_div(rhs).unwrap_or(0),
            server_assignment_count: self.server_assignment_count.checked_div(rhs).unwrap_or(0),
            received: self.received.checked_div(rhs).unwrap_or(0),
            sent: self.sent.checked_div(rhs).unwrap_or(0),
            xact_time: self.xact_time.checked_div(rhs as u32).unwrap_or_default(),
            idle_xact_time: self
                .idle_xact_time
                .checked_div(rhs as u32)
                .unwrap_or_default(),
            query_time: self.query_time.checked_div(rhs as u32).unwrap_or_default(),
            wait_time: self.wait_time.checked_div(rhs as u32).unwrap_or_default(),
            parse_count: self.parse_count.checked_div(rhs).unwrap_or(0),
            bind_count: self.bind_count.checked_div(rhs).unwrap_or(0),
            rollbacks: self.rollbacks.checked_div(rhs).unwrap_or(0),
            healthchecks: self.healthchecks.checked_div(rhs).unwrap_or(0),
            close: self.close.checked_div(rhs).unwrap_or(0),
            errors: self.errors.checked_div(rhs).unwrap_or(0),
            cleaned: self.cleaned.checked_div(rhs).unwrap_or(0),
            prepared_sync: self.prepared_sync.checked_div(rhs).unwrap_or(0),
            connect_time: self
                .connect_time
                .checked_div(rhs as u32)
                .unwrap_or_default(),
            connect_count: self.connect_count.checked_div(rhs).unwrap_or(0),
        }
    }
}

impl Add<BackendCounts> for Counts {
    type Output = Counts;

    fn add(self, rhs: BackendCounts) -> Self::Output {
        Counts {
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

impl Sum for Counts {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        let mut result = Counts::default();
        for next in iter {
            result = result + next;
        }

        result
    }
}

impl Add for Counts {
    type Output = Counts;

    fn add(self, rhs: Self) -> Self::Output {
        Counts {
            xact_count: self.xact_count.saturating_add(rhs.xact_count),
            xact_2pc_count: self.xact_2pc_count.saturating_add(rhs.xact_2pc_count),
            query_count: self.query_count.saturating_add(rhs.query_count),
            server_assignment_count: self
                .server_assignment_count
                .saturating_add(rhs.server_assignment_count),
            received: self.received.saturating_add(rhs.received),
            sent: self.sent.saturating_add(rhs.sent),
            xact_time: self.xact_time.saturating_add(rhs.xact_time),
            idle_xact_time: self.idle_xact_time.saturating_add(rhs.idle_xact_time),
            query_time: self.query_time.saturating_add(rhs.query_time),
            wait_time: self.wait_time.saturating_add(rhs.wait_time),
            parse_count: self.parse_count.saturating_add(rhs.parse_count),
            bind_count: self.bind_count.saturating_add(rhs.bind_count),
            rollbacks: self.rollbacks.saturating_add(rhs.rollbacks),
            healthchecks: self.healthchecks.saturating_add(rhs.healthchecks),
            close: self.close.saturating_add(rhs.close),
            errors: self.errors.saturating_add(rhs.errors),
            cleaned: self.cleaned.saturating_add(rhs.cleaned),
            prepared_sync: self.prepared_sync.saturating_add(rhs.prepared_sync),
            connect_count: self.connect_count.saturating_add(rhs.connect_count),
            connect_time: self.connect_time.saturating_add(rhs.connect_time),
        }
    }
}

#[derive(Debug, Clone, Default, Copy)]
pub struct Stats {
    // Total counts.
    pub counts: Counts,
    /// Counts since last average calculation.
    last_counts: Counts,
    // Average counts.
    pub averages: Counts,
}

impl Stats {
    /// Calculate averages.
    pub fn calc_averages(&mut self, time: Duration) {
        let secs = time.as_secs() as usize;
        if secs > 0 {
            let diff = self.counts - self.last_counts;
            self.averages = diff / secs;
            self.averages.query_time =
                diff.query_time / diff.query_count.try_into().unwrap_or(u32::MAX);
            self.averages.xact_time =
                diff.xact_time / diff.xact_count.try_into().unwrap_or(u32::MAX);
            self.averages.wait_time =
                diff.wait_time / diff.server_assignment_count.try_into().unwrap_or(u32::MAX);
            self.averages.connect_time =
                diff.connect_time / diff.connect_count.try_into().unwrap_or(u32::MAX);
            let queries_in_xact = diff
                .query_count
                .wrapping_sub(diff.xact_count)
                .clamp(1, u32::MAX as usize);
            self.averages.idle_xact_time =
                diff.idle_xact_time / queries_in_xact.try_into().unwrap_or(u32::MAX);

            self.last_counts = self.counts;
        }
    }
}

/// Statistics calculated for the network buffer used
/// by clients and servers.
#[derive(Debug, Clone, Default, Copy)]
pub struct MemoryStats {
    /// Message buffer stats.
    pub buffer: MessageBufferStats,
    /// Memory used by prepared statements.
    pub prepared_statements: usize,
    /// Memory used by the network stream buffer.
    pub stream: usize,
}

impl MemoryStats {
    /// Create new memory stats tracker.
    pub fn new(config: &Memory) -> Self {
        Self {
            buffer: MessageBufferStats {
                bytes_alloc: config.message_buffer,
                ..Default::default()
            },
            prepared_statements: 0,
            stream: config.net_buffer,
        }
    }

    /// Calculate total memory usage.
    pub fn total(&self) -> usize {
        self.buffer.bytes_alloc + self.prepared_statements + self.stream
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_trait() {
        let a = Counts {
            xact_count: 10,
            xact_2pc_count: 5,
            query_count: 20,
            server_assignment_count: 3,
            received: 1000,
            sent: 2000,
            xact_time: Duration::from_secs(5),
            idle_xact_time: Duration::from_secs(3),
            query_time: Duration::from_secs(3),
            wait_time: Duration::from_secs(2),
            parse_count: 15,
            bind_count: 18,
            rollbacks: 2,
            healthchecks: 1,
            close: 4,
            errors: 3,
            cleaned: 6,
            prepared_sync: 7,
            connect_time: Duration::from_secs(1),
            connect_count: 8,
        };

        let b = Counts {
            xact_count: 5,
            xact_2pc_count: 3,
            query_count: 10,
            server_assignment_count: 2,
            received: 500,
            sent: 1000,
            xact_time: Duration::from_secs(2),
            idle_xact_time: Duration::from_secs(5),
            query_time: Duration::from_secs(1),
            wait_time: Duration::from_secs(1),
            parse_count: 8,
            bind_count: 9,
            rollbacks: 1,
            healthchecks: 2,
            close: 3,
            errors: 1,
            cleaned: 2,
            prepared_sync: 3,
            connect_time: Duration::from_secs(2),
            connect_count: 4,
        };

        let result = a + b;

        assert_eq!(result.xact_count, 15);
        assert_eq!(result.xact_2pc_count, 8);
        assert_eq!(result.query_count, 30);
        assert_eq!(result.server_assignment_count, 5);
        assert_eq!(result.received, 1500);
        assert_eq!(result.sent, 3000);
        assert_eq!(result.xact_time, Duration::from_secs(7));
        assert_eq!(result.idle_xact_time, Duration::from_secs(8));
        assert_eq!(result.query_time, Duration::from_secs(4));
        assert_eq!(result.wait_time, Duration::from_secs(3));
        assert_eq!(result.parse_count, 23);
        assert_eq!(result.bind_count, 27);
        assert_eq!(result.rollbacks, 3);
        assert_eq!(result.healthchecks, 3);
        assert_eq!(result.close, 7);
        assert_eq!(result.errors, 4);
        assert_eq!(result.cleaned, 8);
        assert_eq!(result.prepared_sync, 10);
        assert_eq!(result.connect_time, Duration::from_secs(3));
        assert_eq!(result.connect_count, 12);
    }

    #[test]
    fn test_sub_trait() {
        let a = Counts {
            xact_count: 10,
            xact_2pc_count: 5,
            query_count: 20,
            server_assignment_count: 3,
            received: 1000,
            sent: 2000,
            xact_time: Duration::from_secs(5),
            idle_xact_time: Duration::from_secs(3),
            query_time: Duration::from_secs(3),
            wait_time: Duration::from_secs(2),
            parse_count: 15,
            bind_count: 18,
            rollbacks: 2,
            healthchecks: 4,
            close: 5,
            errors: 3,
            cleaned: 6,
            prepared_sync: 7,
            connect_time: Duration::from_secs(3),
            connect_count: 8,
        };

        let b = Counts {
            xact_count: 5,
            xact_2pc_count: 3,
            query_count: 10,
            server_assignment_count: 2,
            received: 500,
            sent: 1000,
            xact_time: Duration::from_secs(2),
            idle_xact_time: Duration::from_secs(2),
            query_time: Duration::from_secs(1),
            wait_time: Duration::from_secs(1),
            parse_count: 8,
            bind_count: 9,
            rollbacks: 1,
            healthchecks: 2,
            close: 3,
            errors: 1,
            cleaned: 2,
            prepared_sync: 3,
            connect_time: Duration::from_secs(1),
            connect_count: 4,
        };

        let result = a - b;

        assert_eq!(result.xact_count, 5);
        assert_eq!(result.xact_2pc_count, 2);
        assert_eq!(result.query_count, 10);
        assert_eq!(result.server_assignment_count, 1);
        assert_eq!(result.received, 500);
        assert_eq!(result.sent, 1000);
        assert_eq!(result.xact_time, Duration::from_secs(3));
        assert_eq!(result.idle_xact_time, Duration::from_secs(1));
        assert_eq!(result.query_time, Duration::from_secs(2));
        assert_eq!(result.wait_time, Duration::from_secs(1));
        assert_eq!(result.parse_count, 7);
        assert_eq!(result.bind_count, 9);
        assert_eq!(result.rollbacks, 1);
        assert_eq!(result.healthchecks, 2);
        assert_eq!(result.close, 2);
        assert_eq!(result.errors, 2);
        assert_eq!(result.cleaned, 4);
        assert_eq!(result.prepared_sync, 4);
        assert_eq!(result.connect_time, Duration::from_secs(2));
        assert_eq!(result.connect_count, 4);
    }

    #[test]
    fn test_sub_trait_saturating() {
        let a = Counts {
            xact_count: 5,
            bind_count: 3,
            ..Default::default()
        };

        let b = Counts {
            xact_count: 10,
            bind_count: 5,
            ..Default::default()
        };

        let result = a - b;

        assert_eq!(result.xact_count, 0);
        assert_eq!(result.bind_count, 0);
    }

    #[test]
    fn test_div_trait() {
        let a = Counts {
            xact_count: 10,
            xact_2pc_count: 6,
            query_count: 20,
            server_assignment_count: 4,
            received: 1000,
            sent: 2000,
            xact_time: Duration::from_secs(10),
            idle_xact_time: Duration::from_secs(20),
            query_time: Duration::from_secs(6),
            wait_time: Duration::from_secs(4),
            parse_count: 15,
            bind_count: 18,
            rollbacks: 2,
            healthchecks: 8,
            close: 12,
            errors: 3,
            cleaned: 6,
            prepared_sync: 9,
            connect_time: Duration::from_secs(8),
            connect_count: 4,
        };

        let result = a / 2;

        assert_eq!(result.xact_count, 5);
        assert_eq!(result.xact_2pc_count, 3);
        assert_eq!(result.query_count, 10);
        assert_eq!(result.server_assignment_count, 2);
        assert_eq!(result.received, 500);
        assert_eq!(result.sent, 1000);
        assert_eq!(result.xact_time, Duration::from_secs(5));
        assert_eq!(result.idle_xact_time, Duration::from_secs(10));
        assert_eq!(result.query_time, Duration::from_secs(3));
        assert_eq!(result.wait_time, Duration::from_secs(2));
        assert_eq!(result.parse_count, 7);
        assert_eq!(result.bind_count, 9);
        assert_eq!(result.rollbacks, 1);
        assert_eq!(result.healthchecks, 4);
        assert_eq!(result.close, 6);
        assert_eq!(result.errors, 1);
        assert_eq!(result.cleaned, 3);
        assert_eq!(result.prepared_sync, 4);
        assert_eq!(result.connect_time, Duration::from_secs(4));
        assert_eq!(result.connect_count, 2);
    }

    #[test]
    fn test_div_by_zero() {
        let a = Counts {
            xact_count: 10,
            xact_time: Duration::from_secs(10),
            ..Default::default()
        };

        let result = a / 0;

        assert_eq!(result.xact_count, 0);
        assert_eq!(result.xact_time, Duration::ZERO);
    }

    #[test]
    fn test_add_backend_counts() {
        let pool_counts = Counts {
            xact_count: 10,
            xact_2pc_count: 5,
            query_count: 20,
            server_assignment_count: 3,
            received: 1000,
            sent: 2000,
            xact_time: Duration::from_secs(5),
            idle_xact_time: Duration::from_secs(10),
            query_time: Duration::from_secs(3),
            wait_time: Duration::from_secs(2),
            parse_count: 15,
            bind_count: 18,
            rollbacks: 2,
            healthchecks: 1,
            close: 4,
            errors: 3,
            cleaned: 6,
            prepared_sync: 7,
            connect_time: Duration::from_secs(1),
            connect_count: 8,
        };

        let backend_counts = BackendCounts {
            bytes_sent: 500,
            bytes_received: 300,
            transactions: 5,
            transactions_2pc: 2,
            queries: 10,
            rollbacks: 1,
            errors: 2,
            prepared_statements: 0,
            query_time: Duration::from_secs(2),
            transaction_time: Duration::from_secs(3),
            idle_in_transaction_time: Duration::from_secs(5),
            parse: 7,
            bind: 8,
            healthchecks: 3,
            close: 2,
            cleaned: 4,
            prepared_sync: 5,
        };

        let result = pool_counts + backend_counts;

        assert_eq!(result.xact_count, 15);
        assert_eq!(result.xact_2pc_count, 7);
        assert_eq!(result.query_count, 30);
        assert_eq!(result.server_assignment_count, 3);
        assert_eq!(result.received, 1300);
        assert_eq!(result.sent, 2500);
        assert_eq!(result.xact_time, Duration::from_secs(8));
        assert_eq!(result.idle_xact_time, Duration::from_secs(15));
        assert_eq!(result.query_time, Duration::from_secs(5));
        assert_eq!(result.wait_time, Duration::from_secs(2));
        assert_eq!(result.parse_count, 22);
        assert_eq!(result.bind_count, 26);
        assert_eq!(result.rollbacks, 3);
        assert_eq!(result.healthchecks, 4);
        assert_eq!(result.close, 6);
        assert_eq!(result.errors, 5);
        assert_eq!(result.cleaned, 10);
        assert_eq!(result.prepared_sync, 12);
        assert_eq!(result.connect_count, 8);
        assert_eq!(result.connect_time, Duration::from_secs(1));
    }

    #[test]
    fn test_calc_averages() {
        let mut stats = Stats::default();

        stats.counts.query_count = 10;
        stats.counts.query_time = Duration::from_millis(500);
        stats.counts.xact_count = 5;
        stats.counts.xact_time = Duration::from_millis(1000);
        stats.counts.server_assignment_count = 4;
        stats.counts.wait_time = Duration::from_millis(200);
        stats.counts.connect_count = 2;
        stats.counts.connect_time = Duration::from_millis(100);

        stats.counts.idle_xact_time = Duration::from_millis(250);

        stats.calc_averages(Duration::from_secs(1));

        assert_eq!(stats.averages.query_time, Duration::from_millis(50));
        assert_eq!(stats.averages.xact_time, Duration::from_millis(200));
        assert_eq!(stats.averages.wait_time, Duration::from_millis(50));
        assert_eq!(stats.averages.connect_time, Duration::from_millis(50));
        // idle_xact_time is divided by (query_count - xact_count) = 10 - 5 = 5
        assert_eq!(stats.averages.idle_xact_time, Duration::from_millis(50));
    }
}
