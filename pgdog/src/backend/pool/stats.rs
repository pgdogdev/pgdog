//!
//! Pool statistics.
//!
//! Used in SHOW POOLS admin database command
//! and in Prometheus metrics.
//!

use crate::config::Memory;

use std::{
    iter::Sum,
    ops::{Add, Deref, DerefMut, Div, Sub},
    time::Duration,
};

use pgdog_stats::memory::MemoryStats as StatsMemoryStats;
use pgdog_stats::pool::Counts as StatsCounts;
use pgdog_stats::pool::Stats as StatsStats;
use pgdog_stats::MessageBufferStats;

/// Pool statistics.
///
/// These are updated after each connection check-in.
///
#[derive(Debug, Clone, Default, Copy)]
pub struct Counts {
    inner: StatsCounts,
}

impl Deref for Counts {
    type Target = StatsCounts;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Counts {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl From<StatsCounts> for Counts {
    fn from(value: pgdog_stats::pool::Counts) -> Self {
        Counts { inner: value }
    }
}

impl Sub for Counts {
    type Output = Counts;

    fn sub(self, rhs: Self) -> Self::Output {
        (self.inner - rhs.inner).into()
    }
}

impl Div<usize> for Counts {
    type Output = Counts;

    fn div(self, rhs: usize) -> Self::Output {
        (self.inner / rhs).into()
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
        (self.inner + rhs.inner).into()
    }
}

#[derive(Debug, Clone, Default, Copy)]
pub struct Stats {
    inner: StatsStats,
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

impl Stats {
    /// Calculate averages.
    pub fn calc_averages(&mut self, time: Duration) {
        self.inner.calc_averages(time);
    }
}

/// Statistics calculated for the network buffer used
/// by clients and servers.
#[derive(Debug, Clone, Default, Copy)]
pub struct MemoryStats {
    pub inner: StatsMemoryStats,
}

impl Deref for MemoryStats {
    type Target = StatsMemoryStats;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for MemoryStats {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl MemoryStats {
    /// Create new memory stats tracker.
    pub fn new(config: &Memory) -> Self {
        Self {
            inner: StatsMemoryStats {
                buffer: MessageBufferStats {
                    bytes_alloc: config.message_buffer,
                    ..Default::default()
                },
                prepared_statements: 0,
                stream: config.net_buffer,
            },
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
        let a: Counts = StatsCounts {
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
            reads: 25,
            writes: 50,
        }
        .into();

        let b: Counts = StatsCounts {
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
            reads: 10,
            writes: 20,
        }
        .into();

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
        assert_eq!(result.reads, 35);
        assert_eq!(result.writes, 70);
    }

    #[test]
    fn test_sub_trait() {
        let a: Counts = StatsCounts {
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
            reads: 25,
            writes: 50,
        }
        .into();

        let b: Counts = StatsCounts {
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
            reads: 10,
            writes: 20,
        }
        .into();

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
        assert_eq!(result.reads, 15);
        assert_eq!(result.writes, 30);
    }

    #[test]
    fn test_sub_trait_saturating() {
        let a: Counts = StatsCounts {
            xact_count: 5,
            bind_count: 3,
            ..Default::default()
        }
        .into();

        let b: Counts = StatsCounts {
            xact_count: 10,
            bind_count: 5,
            ..Default::default()
        }
        .into();

        let result = a - b;

        assert_eq!(result.xact_count, 0);
        assert_eq!(result.bind_count, 0);
    }

    #[test]
    fn test_div_trait() {
        let a: Counts = StatsCounts {
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
            reads: 10,
            writes: 20,
        }
        .into();

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
        assert_eq!(result.reads, 5);
        assert_eq!(result.writes, 10);
    }

    #[test]
    fn test_div_by_zero() {
        let a: Counts = StatsCounts {
            xact_count: 10,
            xact_time: Duration::from_secs(10),
            ..Default::default()
        }
        .into();

        let result = a / 0;

        assert_eq!(result.xact_count, 0);
        assert_eq!(result.xact_time, Duration::ZERO);
    }

    #[test]
    fn test_add_backend_counts() {
        let pool_counts: Counts = StatsCounts {
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
            reads: 10,
            writes: 25,
        }
        .into();

        let backend_counts = pgdog_stats::server::Counts {
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

        let result = pool_counts.inner + backend_counts;

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
        assert_eq!(result.reads, 10);
        assert_eq!(result.writes, 25);
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

    #[test]
    fn test_calc_averages_division_by_zero() {
        let mut stats = Stats::default();

        // Set some time values but leave counts at zero
        stats.counts.query_time = Duration::from_millis(100);
        stats.counts.xact_time = Duration::from_millis(200);
        stats.counts.wait_time = Duration::from_millis(50);
        stats.counts.connect_time = Duration::from_millis(25);
        stats.counts.idle_xact_time = Duration::from_millis(75);

        // Should not panic - clamp(1, u32::MAX) prevents division by zero
        stats.calc_averages(Duration::from_secs(1));

        // When counts are zero, times are divided by 1 (the clamped minimum)
        assert_eq!(stats.averages.query_time, Duration::from_millis(100));
        assert_eq!(stats.averages.xact_time, Duration::from_millis(200));
        assert_eq!(stats.averages.wait_time, Duration::from_millis(50));
        assert_eq!(stats.averages.connect_time, Duration::from_millis(25));
        assert_eq!(stats.averages.idle_xact_time, Duration::from_millis(75));
    }
}
