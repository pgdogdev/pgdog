//! Pool stats.

use crate::backend::stats::Counts as BackendCounts;

use std::{
    iter::Sum,
    ops::{Add, Div, Sub},
    time::Duration,
};

#[derive(Debug, Clone, Default, Copy)]
pub struct Counts {
    pub xact_count: usize,
    pub query_count: usize,
    pub server_assignment_count: usize,
    pub received: usize,
    pub sent: usize,
    pub xact_time: Duration,
    pub query_time: Duration,
    pub wait_time: Duration,
    pub parse_count: usize,
    pub bind_count: usize,
    pub rollbacks: usize,
}

impl Sub for Counts {
    type Output = Counts;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            xact_count: self.xact_count.saturating_sub(rhs.xact_count),
            query_count: self.query_count.saturating_sub(rhs.query_count),
            server_assignment_count: self
                .server_assignment_count
                .saturating_sub(rhs.server_assignment_count),
            received: self.received.saturating_sub(rhs.received),
            sent: self.sent.saturating_sub(rhs.sent),
            xact_time: self.xact_time.saturating_sub(rhs.xact_time),
            query_time: self.query_time.saturating_sub(rhs.query_time),
            wait_time: self.wait_time.saturating_sub(rhs.wait_time),
            parse_count: self.parse_count.saturating_sub(rhs.parse_count),
            bind_count: self.parse_count.saturating_sub(rhs.bind_count),
            rollbacks: self.rollbacks.saturating_sub(rhs.rollbacks),
        }
    }
}

impl Div<usize> for Counts {
    type Output = Counts;

    fn div(self, rhs: usize) -> Self::Output {
        Self {
            xact_count: self.xact_count.saturating_div(rhs),
            query_count: self.query_count.saturating_div(rhs),
            server_assignment_count: self.server_assignment_count.saturating_div(rhs),
            received: self.received.saturating_div(rhs),
            sent: self.sent.saturating_div(rhs),
            xact_time: self.xact_time.checked_div(rhs as u32).unwrap_or_default(),
            query_time: self.query_time.checked_div(rhs as u32).unwrap_or_default(),
            wait_time: self.wait_time.checked_div(rhs as u32).unwrap_or_default(),
            parse_count: self.parse_count.saturating_div(rhs),
            bind_count: self.parse_count.saturating_div(rhs),
            rollbacks: self.rollbacks.saturating_div(rhs),
        }
    }
}

impl Add<BackendCounts> for Counts {
    type Output = Counts;

    fn add(self, rhs: BackendCounts) -> Self::Output {
        Counts {
            xact_count: self.xact_count + rhs.transactions,
            query_count: self.query_count + rhs.queries,
            server_assignment_count: self.server_assignment_count + 1,
            received: self.received + rhs.bytes_received,
            sent: self.sent + rhs.bytes_sent,
            query_time: self.query_time + rhs.query_time,
            xact_time: self.xact_time + rhs.transaction_time,
            wait_time: self.wait_time,
            parse_count: self.parse_count + rhs.parse,
            bind_count: self.parse_count + rhs.bind,
            rollbacks: self.rollbacks + rhs.rollbacks,
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
            query_count: self.query_count.saturating_add(rhs.query_count),
            server_assignment_count: self
                .server_assignment_count
                .saturating_add(rhs.server_assignment_count),
            received: self.received.saturating_add(rhs.received),
            sent: self.sent.saturating_add(rhs.sent),
            xact_time: self.xact_time.saturating_add(rhs.xact_time),
            query_time: self.query_time.saturating_add(rhs.query_time),
            wait_time: self.wait_time.saturating_add(rhs.wait_time),
            parse_count: self.parse_count.saturating_add(rhs.parse_count),
            bind_count: self.parse_count.saturating_add(rhs.bind_count),
            rollbacks: self.rollbacks.saturating_add(rhs.rollbacks),
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
