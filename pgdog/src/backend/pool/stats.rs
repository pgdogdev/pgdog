//! Pool stats.

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
    pub xact_time: usize,
    pub query_time: usize,
    pub wait_time: u128,
}

impl Sub for Counts {
    type Output = Counts;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            xact_count: self.xact_time.saturating_sub(rhs.xact_time),
            query_count: self.query_count.saturating_sub(rhs.query_count),
            server_assignment_count: self
                .server_assignment_count
                .saturating_sub(rhs.server_assignment_count),
            received: self.received.saturating_sub(rhs.received),
            sent: self.sent.saturating_sub(rhs.sent),
            xact_time: self.xact_time.saturating_sub(rhs.xact_time),
            query_time: self.query_time.saturating_sub(rhs.query_time),
            wait_time: self.wait_time.saturating_sub(rhs.wait_time),
        }
    }
}

impl Div<usize> for Counts {
    type Output = Counts;

    fn div(self, rhs: usize) -> Self::Output {
        Self {
            xact_count: self.xact_time.saturating_div(rhs),
            query_count: self.query_count.saturating_div(rhs),
            server_assignment_count: self.server_assignment_count.saturating_div(rhs),
            received: self.received.saturating_div(rhs),
            sent: self.sent.saturating_div(rhs),
            xact_time: self.xact_time.saturating_div(rhs),
            query_time: self.query_time.saturating_div(rhs),
            wait_time: self.wait_time.saturating_div(rhs as u128),
        }
    }
}

impl Add<crate::backend::stats::Counts> for Counts {
    type Output = Counts;

    fn add(self, rhs: crate::backend::stats::Counts) -> Self::Output {
        Counts {
            xact_count: self.xact_count.saturating_add(rhs.transactions),
            query_count: self.query_count.saturating_add(rhs.queries),
            server_assignment_count: self.server_assignment_count + 1,
            received: self.received.saturating_add(rhs.bytes_received),
            sent: self.sent.saturating_add(rhs.bytes_sent),
            query_time: self.query_time,
            xact_time: self.xact_time,
            wait_time: self.wait_time,
        }
    }
}

impl Sum for Counts {
    fn sum<I: Iterator<Item = Self>>(mut iter: I) -> Self {
        let mut result = Counts::default();
        while let Some(next) = iter.next() {
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
