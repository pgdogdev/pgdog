//!
//! Pool statistics.
//!
//! Used in SHOW POOLS admin database command
//! and in Prometheus metrics.
//!

use crate::config::Memory;

use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use pgdog_stats::MessageBufferStats;
use pgdog_stats::memory::MemoryStats as StatsMemoryStats;
use pgdog_stats::pool::Stats as StatsStats;

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
}

#[cfg(test)]
mod tests {
    use super::*;

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

        stats.counts.reads = 30;
        stats.counts.writes = 20;

        stats.calc_averages(Duration::from_secs(1));

        assert_eq!(stats.averages.query_time, Duration::from_millis(50));
        assert_eq!(stats.averages.xact_time, Duration::from_millis(200));
        assert_eq!(stats.averages.wait_time, Duration::from_millis(50));
        assert_eq!(stats.averages.connect_time, Duration::from_millis(50));
        // idle_xact_time is divided by (query_count - xact_count) = 10 - 5 = 5
        assert_eq!(stats.averages.idle_xact_time, Duration::from_millis(50));
        // reads/writes: first divided by secs (30/1=30, 20/1=20), then by xact_count (30/5=6, 20/5=4)
        assert_eq!(stats.averages.reads, 6);
        assert_eq!(stats.averages.writes, 4);
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
