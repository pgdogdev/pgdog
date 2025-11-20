//! Pool stats.

use crate::{backend::stats::Counts as BackendCounts, config::Memory, net::MessageBufferStats};

use std::{
    iter::Sum,
    ops::{Add, Div, Sub},
    time::Duration,
};

#[derive(Debug, Clone, Default, Copy)]
pub struct Counts {
    pub xact_count: usize,
    pub xact_2pc_count: usize,
    pub query_count: usize,
    pub server_assignment_count: usize,
    pub received: usize,
    pub sent: usize,
    pub xact_time: Duration,
    pub idle_xact_time: Duration,
    pub query_time: Duration,
    pub wait_time: Duration,
    pub parse_count: usize,
    pub bind_count: usize,
    pub rollbacks: usize,
    pub healthchecks: usize,
    pub close: usize,
    pub errors: usize,
    pub cleaned: usize,
    pub prepared_sync: usize,
    pub connect_time: Duration,
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
    last_counts: Counts,
    // Average counts.
    pub averages: Counts,
}

impl Stats {
    /// Calculate averages.
    pub fn calc_averages(&mut self, time: Duration) {
        let secs = time.as_secs() as usize;
        if secs > 0 {
            self.averages = (self.counts - self.last_counts) / secs;
            self.last_counts = self.counts;
        }
    }
}

#[derive(Debug, Clone, Default, Copy)]
pub struct MemoryStats {
    pub buffer: MessageBufferStats,
    pub prepared_statements: usize,
    pub stream: usize,
}

impl MemoryStats {
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
}
